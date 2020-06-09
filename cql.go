package main

const CREATE_KS = `CREATE KEYSPACE IF NOT EXISTS lightest
WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor' : 1 }
AND DURABLE_WRITES=true`

const CREATE_SETTINGS_TAB = `
CREATE TABLE lightest.settings (
     key TEXT, -- arbitrary setting name
     ban TEXT, -- arbitrary setting value
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

const INSERT_ACCOUNT = `
INSERT INTO accounts (bic, ban, balance) VALUES (?, ?, ?) IF NOT EXISTS
`

const INSERT_TRANSFER = `
INSERT INTO transfers
  (transfer_id, src_bic, src_ban, dst_bic, dst_ban, amount, state)
  VALUES (?, ?, ?, ?, ?, ?, 'new')
  IF NOT EXISTS
`

const UPDATE_TRANSFER = `
UPDATE transfers USING TTL 30
  SET client_id = ?
  WHERE transfer_id = ?
  IF client_id = ?
`

// Because of a Cassandra/Scylla bug we can't supply NULL as a parameter marker
const UPDATE_TRANSFER_IF_CLIENT_NULL = `
UPDATE transfers USING TTL 30
  SET client_id = ?
  WHERE transfer_id = ?
  IF client_id = NULL
`

const UPDATE_TRANSFER_STATE = `
UPDATE transfers
  SET state = ?
  WHERE transfer_id = ?
  IF client_id = ?
`

const DELETE_TRANSFER = `
DELETE FROM transfers
  WHERE transfer_id = ?
  IF client_id = ?
`

const FETCH_TRANSFER = `
SELECT transfer_id, src_bic, src_ban, dst_bic, dst_ban, amount, state, client_id
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

// Condition balance column simply to get it back
const LOCK_ACCOUNT = `
UPDATE accounts
  SET pending_transfer = ?
  WHERE bic = ? AND ban = ?
  IF pending_transfer = ? AND balance != null
`

const FETCH_BALANCE = `
UPDATE accounts
  SET balance = null
  WHERE bic = ? AND ban = ?
  IF balance = null
`

const UPDATE_BALANCE = `
UPDATE accounts
  SET pending_transfer = null, balance = ?
  WHERE bic = ? AND ban = ?
  IF pending_transfer = ?
`

const CHECK_BALANCE = `
SELECT SUM(balance)
  FROM accounts
`

const DROP_KS = `
DROP KEYSPACE IF EXISTS lightest
`
