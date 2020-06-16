package main

const CREATE_KS = `CREATE KEYSPACE IF NOT EXISTS lightest
WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor' : 1 }
AND DURABLE_WRITES=true`

const CREATE_SETTINGS_TAB = `
CREATE TABLE lightest.settings (
     key TEXT, -- arbitrary setting name
     value TEXT, -- arbitrary setting value
     PRIMARY KEY((key))
)`

const CREATE_ACCOUNTS_TAB = `
CREATE TABLE lightest.accounts (
     bic TEXT, -- bank identifier code
     ban TEXT, -- bank account number within the bank
     balance DECIMAL, -- account balance
     pending_transfer UUID, -- will be used later
     PRIMARY KEY((bic, ban))
)`

const CREATE_TRANSFERS_TAB = `
CREATE TABLE lightest.transfers (
    transfer_id UUID, -- transfers UUID
    src_bic TEXT, -- source bank identification code
    src_ban TEXT, -- source bank account number
    dst_bic TEXT, -- destination bank identification code
    dst_ban TEXT, -- destination bank account number
    amount DECIMAL, -- transfer amount
    state TEXT, -- ‘pending’, ‘in progress’, ‘complete’
    client_id UUID, -- the client performing the transfer
    PRIMARY KEY (transfer_id)
)`

const CREATE_CHECK_TAB = `
CREATE TABLE lightest.check (
	name TEXT,
	amount DECIMAL,
	PRIMARY KEY(name)
)`

const INSERT_SETTING = `
INSERT INTO lightest.settings (key, value) VALUES (?, ?)
`

const FETCH_SETTING = `
SELECT value FROM lightest.settings WHERE key = ?
`

const INSERT_ACCOUNT = `
INSERT INTO accounts (bic, ban, balance) VALUES (?, ?, ?) IF NOT EXISTS
`

// Client id has to be updated separately to let it expire
const INSERT_TRANSFER = `
INSERT INTO transfers
  (transfer_id, src_bic, src_ban, dst_bic, dst_ban, amount, state)
  VALUES (?, ?, ?, ?, ?, ?, 'new')
  IF NOT EXISTS
`

// Because of a Cassandra/Scylla bug we can't supply NULL as a parameter marker
// Always check the row exists to not accidentally add a transfer
const SET_TRANSFER_CLIENT = `
UPDATE transfers USING TTL 30
  SET client_id = ?
  WHERE transfer_id = ?
  IF state != NULL AND client_id = NULL
`

// Always check the row exists to not accidentally add a transfer
const CLEAR_TRANSFER_CLIENT = `
UPDATE transfers
  SET client_id = NULL
  WHERE transfer_id = ?
  IF state != NULL AND client_id = ?
`

// Always check the row exists to not accidentally add a transfer
const UPDATE_TRANSFER_STATE = `
UPDATE transfers
  SET state = 'in progress'
  WHERE transfer_id = ?
  IF state = 'new' AND client_id = ?
`

const DELETE_TRANSFER = `
DELETE FROM transfers
  WHERE transfer_id = ?
  IF client_id = ?
`

const FETCH_TRANSFER = `
SELECT src_bic, src_ban, dst_bic, dst_ban, amount, state
  FROM transfers
  WHERE transfer_id = ?
`

const FETCH_TRANSFER_CLIENT = `
SELECT client_id
  FROM transfers
  WHERE transfer_id = ?
`

// Cassandra/Scylla don't handle IF client_id = NUll queries
// correctly. But NULLs are implicitly converted to mintimeuuids
// during comparison. Use one bug to workaround another.
const FETCH_DEAD_TRANSFERS = `
SELECT transfer_id
  FROM transfers
  WHERE client_id < minTimeuuid('1979-08-12 21:35+0000')
  ALLOW FILTERING
`

// Condition balance column:
// 1) To avoid accidentally inserting a new account here
// 2) To get it back (Scylla only)
const LOCK_ACCOUNT = `
UPDATE accounts
  SET pending_transfer = ?
  WHERE bic = ? AND ban = ?
  IF balance != NULL AND pending_transfer = NULL
`

// Always check the row exists in IF to not accidentally add a transfer
const UNLOCK_ACCOUNT = `
UPDATE accounts
  SET pending_transfer = NULL
  WHERE bic = ? AND ban = ?
  IF balance != NULL AND pending_transfer = ?
`

const FETCH_BALANCE = `
SELECT balance
  FROM accounts
  WHERE bic = ? AND ban = ?
`

// Always check the row exists in IF to not accidentally add a transfer
const UPDATE_BALANCE = `
UPDATE accounts
  SET pending_transfer = NULL, balance = ?
  WHERE bic = ? AND ban = ?
  IF balance != NULL AND pending_transfer = ?
`

const CHECK_BALANCE = `
SELECT SUM(balance) FROM accounts
`

const PERSIST_TOTAL = `
UPDATE lightest.check SET amount = ?  WHERE name = 'total'
`

const FETCH_TOTAL = `
SELECT amount FROM lightest.check WHERE name = 'total'
`

const DROP_KS = `
DROP KEYSPACE IF EXISTS lightest
`
