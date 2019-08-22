package cruzbit

import (
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"golang.org/x/crypto/ed25519"
	"log"
	"strconv"
)

// BlockStorageSql uses a MariaDB or MySQL database to store
//  blocks and transactions in a relational database
type BlockStorageSql struct {
	db       *sql.DB
	readOnly bool
	stmtMap  map[string]*sql.Stmt
}

type BlockStorageSqlConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
}

// NewBlockStorageDisk returns a new instance of on-disk block storage.
func NewBlockStorageSql(config BlockStorageSqlConfig, readOnly bool) (*BlockStorageSql, error) {
	// open the database
	dsn := config.Username + ":" + config.Password + "@tcp(" + config.Host +
		":" + strconv.Itoa(config.Port) + ")/" + config.Database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("Error connecting to database: ", err)
	}

	// (optionally) check connection immediately
	err = db.Ping()
	if err != nil {
		log.Fatal("Unable to connect to database: ", err)
	}

	// prepare reusable queries
	statements := make(map[string]*sql.Stmt)
	stmt, err := db.Prepare(
		"SELECT `transaction_id` " +
			"FROM `block_transactions` " +
			"WHERE `block_id` = ? " +
			"ORDER BY `index` ASC")
	statements["get_block_transactions"] = stmt
	stmt, err = db.Prepare(
		"SELECT `previous`, `hash_list_root`, `time`, `target`, " +
			"`chain_work`, `nonce`, `height`, `transaction_count` " +
			"FROM `blocks` " +
			"WHERE `block_id` = ? " +
			"LIMIT 1")
	statements["get_block_header"] = stmt
	stmt, err = db.Prepare(
		"SELECT `transaction_id` " +
			"FROM `block_transactions` " +
			"WHERE `block_id` = ? " +
			"AND `index` = ? " +
			"LIMIT 1")
	statements["get_transaction_id"] = stmt
	stmt, err = db.Prepare("SELECT `time`, `nonce`, `from`, `to`, " +
		"`amount`, `fee`, `memo`, `matures`, `expires`, `series`, `signature` " +
		"FROM `transactions` " +
		"WHERE `transaction_id` = ? " +
		"LIMIT 1")
	statements["get_transaction"] = stmt

	return &BlockStorageSql{
		db:       db,
		readOnly: readOnly,
		stmtMap:  statements,
	}, nil
}

// Close is called to close any underlying storage.
func (b BlockStorageSql) Close() error {
	for _, s := range b.stmtMap {
		s.Close()
	}
	return b.db.Close()
}

// Store is called to store all of the block's information.
func (b BlockStorageSql) Store(id BlockID, block *Block, now int64) error {
	if b.readOnly {
		return fmt.Errorf("Block storage is in read-only mode")
	}

	// start transaction
	tx, err := b.db.Begin()
	if err != nil {
		log.Fatal("Could not open transaction", err)
	}

	// store block
	stmt, err := tx.Prepare(
		"INSERT INTO `blocks` (`block_id`, `previous`, `hash_list_root`, `time`, " +
			"`target`, `chain_work`, `nonce`, `height`, `transaction_count`) " +
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
			"ON DUPLICATE KEY UPDATE block_id = block_id")
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("Store():66 %s", err.Error())
	}
	defer stmt.Close()
	_, err = stmt.Exec(
			id.String(),
			block.Header.Previous.String(),
			block.Header.HashListRoot.String(),
			block.Header.Time,
			block.Header.Target.String(),
			block.Header.ChainWork.String(),
			block.Header.Nonce,
			block.Header.Height,
			block.Header.TransactionCount)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("Store():80 %s", err.Error())
	}

	// store transactions
	for index, tr := range block.Transactions {
		err := b.StoreTransaction(tx, id, block.Header.Height, tr, index)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("Store():88 %s", err.Error())
		}
	}

	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("Store():95 %s", err.Error())
	}
	return nil
}

func (b BlockStorageSql) StoreTransaction(tx *sql.Tx, blockId BlockID, height int64, transaction *Transaction, index int) error {
	if b.readOnly {
		return fmt.Errorf("Block storage is in read-only mode")
	}

	txId, err := transaction.ID()

	// store transaction
	stmt, err := tx.Prepare(
		"INSERT INTO `transactions` (`transaction_id`, `time`, `nonce`, `from`, " +
			"`to`, `amount`, `fee`, `memo`, `matures`, `expires`, `series`, `signature`) " +
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
			"ON DUPLICATE KEY UPDATE transaction_id = transaction_id")
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("StoreTransaction():111 %s", err.Error())
	}
	_, err = stmt.Exec(
		txId.String(),
		transaction.Time,
		transaction.Nonce,
		nullifyString(base64.StdEncoding.EncodeToString(transaction.From)),
		base64.StdEncoding.EncodeToString(transaction.To),
		nullifyInt64(transaction.Amount),
		nullifyInt64(transaction.Fee),
		nullifyString(transaction.Memo),
		nullifyInt64(transaction.Matures),
		nullifyInt64(transaction.Expires),
		transaction.Series,
		nullifyString(base64.StdEncoding.EncodeToString(transaction.Signature)))
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("StoreTransaction():128 %s", err.Error())
	}
	stmt.Close()

	// add to the join table
	stmt, err = tx.Prepare(
		"INSERT INTO `block_transactions` (`height`, `block_id`, `transaction_id`, `index`) " +
		"VALUES (?, ?, ?, ?) " +
		"ON DUPLICATE KEY UPDATE height = height")
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("StoreTransaction():138 %s", err.Error())
	}
	_, err = stmt.Exec(
		height,
		blockId.String(),
		txId.String(),
		index)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("StoreTransaction():147 %s", err.Error())
	}
	stmt.Close()
	return nil
}

// Get returns the referenced block.
func (b BlockStorageSql) GetBlock(id BlockID) (*Block, error) {
	var block Block

	// get block header
	header, _, err := b.GetBlockHeader(id)
	if err != nil {
		log.Fatal("Error getting block header: ", err)
	}
	block.Header = header

	// get transaction ids
	stmt := b.stmtMap["get_block_transactions"]
	if err != nil {
		log.Fatal("Error preparing statement: ", err)
	}
	rows, err := stmt.Query(id.String())
	if err != nil {
		log.Fatal("Error retrieving transactions ID: ", err)
	}
	var txIds []string
	for rows.Next() {
		var txId string
		err := rows.Scan(&txId)
		if err != nil {
			log.Fatal("Error scanning transaction ID: ", err)
		}
		txIds = append(txIds, txId)
	}
	rows.Close()

	// get transactions (non-reusable query)
	query, args, err := sqlx.In("SELECT `time`, `nonce`, `from`, `to`, " +
		"`amount`, `fee`, `memo`, `matures`, `expires`, `series`, `signature`" +
		"FROM `transactions`" +
		"WHERE `transaction_id` IN (?)", txIds)
	if err != nil {
		log.Fatal("Error preparing statement: ", err)
	}
	rows, err = b.db.Query(query, args...)
	if err != nil {
		log.Fatal("Error retrieving block transactions: ", err)
	}
	// add transactions to block
	for rows.Next() {
		var transaction Transaction
		//transaction := new(Transaction)
		var from, to, memo, signature sql.NullString
		var fee, matures, expires, series sql.NullInt64
		err = rows.Scan(
			&transaction.Time,
			&transaction.Nonce,
			&from,
			&to,
			&transaction.Amount,
			&fee,
			&memo,
			&matures,
			&expires,
			&series,
			&signature)
		if err != nil {
			log.Fatal("Error retrieving block transaction: ", err)
		}

		//TODO: need error handling on all of this?
		if from.Valid { transaction.From, err = decodePublicKey(from.String) }
		if to.Valid {
			transaction.To, err = decodePublicKey(to.String)
			if err != nil {
				log.Fatal("Could not decode transaction.To from database: ", err)
			}
		}
		if signature.Valid { transaction.Signature, err = decodeSignature(signature.String) }
		if fee.Valid { transaction.Fee = fee.Int64 }
		if memo.Valid { transaction.Memo = memo.String }
		if matures.Valid { transaction.Matures = matures.Int64 }
		if expires.Valid { transaction.Expires = expires.Int64 }
		if series.Valid { transaction.Series = series.Int64 }
		block.Transactions = append(block.Transactions, &transaction)
	}
	rows.Close()
	return &block, nil
}

// GetBlockBytes returns the referenced block as a byte slice.
func (b BlockStorageSql) GetBlockBytes(id BlockID) ([]byte, error) {
	block, err := b.GetBlock(id)
	if err != nil {
		log.Fatal("Error getting block: ", err)
	}
	bytes, err := json.Marshal(block)
	if err != nil {
		log.Fatal("Error marshalling block: ", err)
	}
	return bytes, nil
}

// GetBlockHeader returns the referenced block's header and the timestamp of when it was stored.
func (b BlockStorageSql) GetBlockHeader(id BlockID) (*BlockHeader, int64, error) {
	header := new(BlockHeader)

	// get block header
	stmt := b.stmtMap["get_block_header"]
	var previous, hashListRoot, target, chainWork string
	err := stmt.QueryRow(id.String()).Scan(
		&previous,
		&hashListRoot,
		&header.Time,
		&target,
		&chainWork,
		&header.Nonce,
		&header.Height,
		&header.TransactionCount)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Println("GetBlockHeader: ", id.String())
			log.Println("GetBlockHeader: No rows found")
			return nil, 0, nil
		} else {
			log.Fatal("Error retrieving block header: ", err)
		}
	}
	if err = decodeBlockID(previous, &header.Previous); err != nil {
		log.Fatal("Could not decode header.Previous from database: ", err)
	}
	if err = decodeTransactionID(hashListRoot, &header.HashListRoot); err != nil {
		log.Fatal("Could not decode header.HashListRoot from database: ", err)
	}
	if err = decodeBlockID(target, &header.Target); err != nil {
		log.Fatal("Could not decode header.Target from database: ", err)
	}
	if err = decodeBlockID(chainWork, &header.ChainWork); err != nil {
		log.Fatal("Could not decode header.ChainWork from database: ", err)
	}

	//TODO: we don't keep the timestamp of when the block was inserted
	// so fake it here? Any problem caused by this?
	return header, header.Time, nil
}

// GetTransaction returns a transaction within a block and the block's header.
func (b BlockStorageSql) GetTransaction(id BlockID, index int) (*Transaction, *BlockHeader, error) {
	// get transaction id
	stmt := b.stmtMap["get_transaction_id"]
	var txId string
	err := stmt.QueryRow(id.String(), index).Scan(&txId)
	if err != nil {
		//return nil, nil, err
		log.Fatal("Error retrieving transaction ID: ", err)
	}

	// get transaction
	stmt = b.stmtMap["get_transaction"]
	var transaction Transaction
	var from, to, memo, signature sql.NullString
	var fee, matures, expires, series sql.NullInt64
	err = stmt.QueryRow(txId).Scan(
		&transaction.Time,
		&transaction.Nonce,
		&from,
		&to,
		&transaction.Amount,
		&fee,
		&memo,
		&matures,
		&expires,
		&series,
		&signature)
	if err != nil {
		log.Fatal("Error retrieving single transaction: ", err)
	}
	if from.Valid { transaction.From, err = decodePublicKey(from.String) }
	if to.Valid { transaction.To, err = decodePublicKey(to.String) }
	if signature.Valid { transaction.Signature, err = decodeSignature(signature.String) }
	if fee.Valid { transaction.Fee = fee.Int64 }
	if memo.Valid { transaction.Memo = memo.String }
	if matures.Valid { transaction.Matures = matures.Int64 }
	if expires.Valid { transaction.Expires = expires.Int64 }
	if series.Valid { transaction.Series = series.Int64 }

	header, _, err := b.GetBlockHeader(id)
	if err != nil {
		//return nil, nil, err
		log.Fatal("Error retrieving transaction: ", err)
	}
	return &transaction, header, nil
}

func decodeBlockID(str string, target *BlockID) error {
	//res, err := base64.StdEncoding.DecodeString(str)
	res, err := hex.DecodeString(str)
	if err != nil {
		return err
	}
	copy(target[:], res)
	return nil
}

func decodeTransactionID(str string, target *TransactionID) error {
	//res, err := base64.StdEncoding.DecodeString(str)
	res, err := hex.DecodeString(str)
	if err != nil {
		return err
	}
	copy(target[:], res)
	return nil
}

func decodePublicKey(str string) (ed25519.PublicKey, error) {
	res, err := base64.StdEncoding.DecodeString(str)
	//res, err := hex.DecodeString(str)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func decodeSignature(str string) (Signature, error) {
	res, err := base64.StdEncoding.DecodeString(str)
	//res, err := hex.DecodeString(str)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func nullifyString(s string) interface{} {
	if len(s) == 0 {
		return sql.NullString{}
	}
	return s
}

func nullifyInt64(i int64) interface{} {
	if i == 0 {
		return sql.NullString{}
	}
	return i
}
