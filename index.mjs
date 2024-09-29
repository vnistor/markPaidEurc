import { KMSClient, DecryptCommand } from "@aws-sdk/client-kms";
import pg from 'pg';
import mongoose from 'mongoose';
import Ledger from './models/ledger.js';
import Users from './models/users.js';
import { Buffer} from 'buffer';
import sendgrid from '@sendgrid/mail'

const encrypted = {
  MONGO: process.env.MONGO,
  PG: process.env.PG,
  SENDGRID: process.env.SENDGRID
};
let decrypted = {};

export const handler = async (event, contxt) => {
  console.log('[markPaidEurc]', '[HANDLER]', event);

  let result;

  try {
    if (decrypted.PG) {
      console.log('[markPaidEurc]', "[HANDLER]", "[DECRYPT]", "Already decrypted");

      result = await startup(event.body);
      result.body = JSON.stringify(result.body);

      return result;
    } else {
      console.log('[markPaidEurc]', "[HANDLER]", "[DECRYPT]", "Starting decryption");
      try {
        const kms = new KMSClient({ region: 'eu-west-1' });
        const decryptMONGO = new DecryptCommand ({ CiphertextBlob: Buffer.from(encrypted.MONGO, 'base64'), EncryptionContext: { LambdaFunctionName: process.env.AWS_LAMBDA_FUNCTION_NAME } });
        const decryptPG = new DecryptCommand ({ CiphertextBlob: Buffer.from(encrypted.PG, 'base64'), EncryptionContext: { LambdaFunctionName: process.env.AWS_LAMBDA_FUNCTION_NAME } });
        const decryptSENDGRID = new DecryptCommand ({ CiphertextBlob: Buffer.from(encrypted.SENDGRID, 'base64'), EncryptionContext: { LambdaFunctionName: process.env.AWS_LAMBDA_FUNCTION_NAME } });

        let decryptMongoRes = await kms.send (decryptMONGO);
        let decryptPgRes = await kms.send (decryptPG);
        let decryptSendgridRes = await kms.send (decryptSENDGRID);

        decrypted.MONGO = Buffer.from(decryptMongoRes.Plaintext).toString('ascii');
        decrypted.PG = Buffer.from(decryptPgRes.Plaintext).toString('ascii');
        decrypted.SENDGRID = Buffer.from(decryptSendgridRes.Plaintext).toString('ascii');


      } catch (err) {
        console.error('[markPaidEurc]', "[HANDLER]", "[DECRYPT]", 'Decrypt error:', err);
        result.statusCode = 500;
        result.error = JSON.stringify(err);
        return result;
      }

      console.log('[markPaidEurc]', "[HANDLER]", "[DECRYPT]", "Decrypted");

      try {

        result = await startup(event.body);
        result.body = JSON.stringify(result.body);

        return result;
      } catch (err) {
        console.error('[markPaidEurc]', "[HANDLER]", err);
        result.statusCode = 500;
        result.error = JSON.stringify(err);
        return result;
      }

    }
  }
  catch (err) {
    console.error('[markPaidEurc]', "[HANDLER]", err);
    result.statusCode = 500;
    result.error = JSON.stringify(err);
    return result;
  }
};

async function startup (data) {

  if (data.id === undefined) {
    data = JSON.parse(data);
  }

  console.log('[markPaidEurc]', data.userid, data.id, "[STARTUP]", "[START]");

  try {

    const pgclient = new pg.Client({
      connectionString: decrypted.PG,
      ssl: { rejectUnauthorized: false }
    });

    const result = await deleteWithdrawal(pgclient, data);
    await pgclient.end();

    return result;

  } catch (err) {
    console.error('[markPaidEurc]', data.userid, data.id, "[STARTUP]", "[ERROR]", err);
    return Error(err);
  }
}

async function deleteWithdrawal (pgclient, data) {

  /* incoming
    data = {
      userid: userid,
      id: id,
      txid: txid
    }
  */

  try {
    console.log('[markPaidEurc]', data);

    await pgclient.connect();
    console.log('[markPaidEurc]', data.userid, data.id, "[PG]", "CONNECTED");

    await pgclient.query('BEGIN');
    console.log('[markPaidEurc]', data.userid, data.id, "[PG]", "BEGIN");

    const selTx = `
    SELECT *
      FROM ledger
        WHERE
          id = $1
        AND
          userid = $2
      LIMIT 1
      FOR UPDATE`;

      const tx = await pgclient.query(selTx, [data.id, data.userid]);

      if (tx.rows.length === 0) {
        console.error('[markPaidEurc]', data.userid, data.id, "[PG]", '[selTx]', "No transaction found");
        await pgclient.query('ROLLBACK');
        return { statusCode: 404, body: { error: "No transaction found" }};
      } else if (tx.rows.length === 1) {
        console.log("[markPaidEurc]", data.userid, data.id, "[PG]", "[selTx]", tx.rows[0]);
      }

      if (Number(tx.rows[0].status) === 4 || tx.rows[0].status === '4') {
        console.error('[markPaidEurc]', data.userid, data.id, "[PG]", '[selTx]', "Transaction already settled");
        await pgclient.query('ROLLBACK');
        return { statusCode: 404, body: { error: "Transaction already settled" }};
      }

      const selTxid = `
      SELECT *
        FROM ledger
          WHERE
            txid = $1
          or
            tref = $1
        LIMIT 1`;

        const txidRes = await pgclient.query(selTxid, [data.txid]);

        if (txidRes.rows.length === 1) {
          console.error('[markPaidEurc]', data.userid, data.id, "[PG]", '[selTxid]', "TXID already used to process transaction.");
          await pgclient.query('ROLLBACK');
          return { statusCode: 404, body: { error: "TXID already used to process transaction." }};
        }

      const updateTx = `
      UPDATE ledger
        SET
          status = 4,
          txid = $3,
          tref = $3,
          set = true,
          settling = true,
          trtime = 'now'
        WHERE
          id = $1
        AND
          userid = $2
      RETURNING *`;

    const updateTxRes = await pgclient.query(updateTx, [tx.rows[0].id, tx.rows[0].userid, data.txid]);

    if (updateTxRes.rows.length === 0) {
      console.error('[markPaidEurc]', data.userid, data.id, "[PG]", '[updateTx]', "No tx found to update");
      await pgclient.query('ROLLBACK');
      return { statusCode: 404, body: { error: "No tx found to update" }};
    } else if (updateTxRes.rows.length === 1) {
      console.log("[markPaidEurc]", data.userid, data.id, "[PG]", "[updateTx]", updateTxRes.rows[0].txid);
    }

    await mongoose.connect(decrypted.MONGO);
    console.log("[markPaidEurc]", data.userid, data.id, "[MONGO]", "CONNECT");

    // UPDATE DATA

    const txMongoUpdate = await Ledger.updateOne(
      {"_id": updateTxRes.rows[0].id, "userid": updateTxRes.rows[0].userid},
      {"trtime": updateTxRes.rows[0].trtime, "settle": updateTxRes.rows[0].set, "settling": updateTxRes.rows[0].settling, "txid": updateTxRes.rows[0].txid, "tref": updateTxRes.rows[0].tref, "status": updateTxRes.rows[0].status }
    );
    console.log("[markPaidEurc]", data.userid, data.id, "[MONGO]", "[updateTx]", txMongoUpdate.modifiedCount);

    const user = await Users.findOne({_id: updateTxRes.rows[0].userid});
    console.log("[markPaidEurc]", data.userid, data.id, "[MONGO]", "[findUser]", user._id);

    const emailAddresses = await getEmails(user.emails);
    if (emailAddresses.length < 1) {
      console.log("[markPaidEurc]", data.userid, data.id, "[EMAIL]", "[getAddress]", "No verified emails");
      await pgclient.query('ROLLBACK');
      return { statusCode: 404, body: { error: "No verified emails" }};
    }
    const message = {
      "to": emailAddresses,
      "from": {"email": "confirmations@coinflux.com", "name": "CoinFlux confirmations"},
      "reply_to": {"email": "support@coinflux.com", "name": "CoinFlux Support"},
      "templateId": "5321cd6e-9e97-40d2-83b5-83ce969bdda8",
      "content": [{"type": 'text/html', "value": " "}],
      "substitutions": {
        "ccy1": updateTxRes.rows[0].ccy1,
        "cost_net": Number(updateTxRes.rows[0].cost_net).toFixed(3).toString(),
        "iban": updateTxRes.rows[0].iban,
        "addr": updateTxRes.rows[0].addr,
        "id": updateTxRes.rows[0].id,
        "time": updateTxRes.rows[0].time,
        "walletid": updateTxRes.rows[0].walletid,
        "bank": updateTxRes.rows[0].bank,
        "txid": updateTxRes.rows[0].txid
      },
      "substitutionWrappers": ['{{', "}}"]
    };

    sendgrid.setApiKey(decrypted.SENDGRID)

    const sendEmailRes = await sendgrid.send(message);
    console.log("[markPaidEurc]", data.userid, data.id, "[EMAIL]", "[SEND]", "OK");

    if (process.env.TESTING === "true") {
      console.log("[markPaidEurc]", data.userid, data.id, "[TESTING]", "CLEANUP");

      const txMongoCleanup = await Ledger.updateOne(
        {"_id": updateTxRes.rows[0].id, "userid": updateTxRes.rows[0].userid},
        {"trtime": tx.rows[0].trtime, "settle": tx.rows[0].set, "settling": tx.rows[0].settling, "txid": tx.rows[0].txid, "tref": tx.rows[0].tref, "status": tx.rows[0].status }
      );
      console.log("[markPaidEurc]", data.userid, data.id, "[MONGO]", "[txMongoCleanup]", txMongoCleanup.modifiedCount);

      await pgclient.query('ROLLBACK');

      await mongoose.disconnect();
      console.log("[markPaidEurc]", data.userid, data.id, "[MONGO]", "DISCONNECT");
      console.log("[markPaidEurc]", data.userid, data.id, "[MONGO]", "OK");

      return { statusCode: 200, body: { id: updateTxRes.rows[0].id }};

    }

    await pgclient.query ("COMMIT");
    console.log("[markPaidEurc]", data.userid, data.id, "[PG]", "[COMMIT]", "OK");
    console.log("[markPaidEurc]", data.userid, data.id, "[PG]", "OK");

    await mongoose.disconnect();
    console.log("[markPaidEurc]", data.userid, data.id, "[MONGO]", "DISCONNECT");
    console.log("[markPaidEurc]", data.userid, data.id, "[MONGO]", "OK");

    return { statusCode: 200, body: { id: updateTxRes.rows[0].id }};


  } catch (err) {
    console.error("[markPaidEurc]", data.userid, data.id, "[OPERATIONS]", err);
    await pgclient.query('ROLLBACK');
    return Error(err);
  }

}

async function getEmails (emails) {
  return emails.filter(x => x.verified).map( (x) => { return {"email":x.address};});
}
