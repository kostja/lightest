package main

const CREATE_KS = `CREATE KEYSPACE IF NOT EXISTS lightest
WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor' : 3 }
AND DURABLE_WRITES=true`

const CREATE_ACCOUNTS_TAB = `
CREATE TABLE accounts (
     bic TEXT, -- bank identifier code
     ban TEXT, -- bank account number within the bank
     balance DECIMAL, -- account balance
     pending_transfer UUID, -- will be used later
     PRIMARY KEY((bic, ban))
)`

const CREATE_TRANSFERS_TAB = `
CREATE TABLE transfers (
    id UUID, -- transfers UUID
    src_bic TEXT, -- source bank identification code
    src_ban TEXT, -- source bank account number
    dst_bic TEXT, -- destination bank identification code
    dst_ban TEXT, -- destination bank account number
    amount DECIMAL, -- transfer amount
    state TEXT, -- ‘pending’, ‘in progress’, ‘complete’
    client UUID, -- the client performing the transfer
    PRIMARY KEY (id)
)`

const INSERT_ACCOUNT = `
INSERT INTO accounts (bic, ban, balance) VALUES (...) IF NOT EXISTS
`

const INSERT_TRANSER = `
INSERT INTO transfers
  (id, src_bic, src_ban, dst_bic, dst_ban, amount, state)
  VALUES (?, ?, ?, ?, ?, ?, ‘pending’)
  IF NOT EXISTS
`

const UPDATE_TRANSFER = `
UPDATE transfers USING TTL 300
  SET client = ?, state = ?
  WHERE id = ?
  IF client = ?
`

const CHECK_SRC_ACCOUNT = `
UPDATE accounts
  SET pending_transfer = ?
  WHERE bic = ? AND ban = ?
  IF pending_transfer = null AND balance > ?
`

const CHECK_DST_ACCOUNT = `
UPDATE accounts
  SET pending_transfer=$uuid
  WHERE bic = ? AND ban = ?
  IF pending_transfer = null
`

const UPDATE_ACCOUNT_SET_BALANCE = `
UPDATE accounts
  SET pending_transfer = null balance = ?
  WHERE bic = ? AND ban = ?
  IF pending_transfer = ?
`
