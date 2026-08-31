package store

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

const secretPrefix = "enc:v1:"

type SecretMetadata struct {
	ID         string    `json:"id"`
	Kind       string    `json:"kind"`
	OwnerID    string    `json:"owner_id"`
	KeyVersion int       `json:"key_version"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func (s *SQLite) PutSecret(ctx context.Context, kind, ownerID, value string) (SecretMetadata, error) {
	if !s.SecretReady() {
		return SecretMetadata{}, ErrSecretUnavailable
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SecretMetadata{}, fmt.Errorf("begin secret write: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	metadata, err := putSecretTx(ctx, tx, s.secretKey, kind, ownerID, value)
	if err != nil {
		return SecretMetadata{}, err
	}
	if err := tx.Commit(); err != nil {
		return SecretMetadata{}, fmt.Errorf("commit secret write: %w", err)
	}
	return metadata, nil
}

func putSecretTx(ctx context.Context, tx *sql.Tx, key []byte, kind, ownerID, value string) (SecretMetadata, error) {
	if len(key) != 32 {
		return SecretMetadata{}, ErrSecretUnavailable
	}
	if !documentDomainPattern.MatchString(kind) || strings.TrimSpace(ownerID) == "" || len(ownerID) > 256 {
		return SecretMetadata{}, fmt.Errorf("%w: invalid secret kind or owner_id", ErrInvalid)
	}
	if value == "" || len(value) > 1<<20 {
		return SecretMetadata{}, fmt.Errorf("%w: secret value must be between 1 byte and 1 MiB", ErrInvalid)
	}
	ciphertext, err := encryptSecret(key, value)
	if err != nil {
		return SecretMetadata{}, err
	}
	now := time.Now().UTC()
	id, err := randomID("sec")
	if err != nil {
		return SecretMetadata{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO control_secrets
		(id, kind, owner_id, ciphertext, key_version, created_at, updated_at)
		VALUES (?, ?, ?, ?, 1, ?, ?)
		ON CONFLICT(kind, owner_id) DO UPDATE SET ciphertext=excluded.ciphertext,
			key_version=excluded.key_version, updated_at=excluded.updated_at`,
		id, kind, ownerID, ciphertext, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano),
	)
	if err != nil {
		return SecretMetadata{}, fmt.Errorf("store secret: %w", err)
	}
	return getSecretMetadataTx(ctx, tx, kind, ownerID)
}

func deleteSecretTx(ctx context.Context, tx *sql.Tx, kind, ownerID string) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM control_secrets WHERE kind = ? AND owner_id = ?`, kind, ownerID)
	return err
}

func getSecretMetadataTx(ctx context.Context, tx *sql.Tx, kind, ownerID string) (SecretMetadata, error) {
	var metadata SecretMetadata
	var createdAt string
	var updatedAt string
	err := tx.QueryRowContext(ctx, `SELECT id, kind, owner_id, key_version, created_at, updated_at
		FROM control_secrets WHERE kind = ? AND owner_id = ?`, kind, ownerID).Scan(
		&metadata.ID, &metadata.Kind, &metadata.OwnerID, &metadata.KeyVersion, &createdAt, &updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SecretMetadata{}, ErrNotFound
	}
	if err != nil {
		return SecretMetadata{}, fmt.Errorf("read secret metadata: %w", err)
	}
	metadata.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return SecretMetadata{}, ErrCorrupt
	}
	metadata.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return SecretMetadata{}, ErrCorrupt
	}
	return metadata, nil
}

func (s *SQLite) GetSecret(ctx context.Context, kind, ownerID string) (SecretMetadata, string, error) {
	if !s.SecretReady() {
		return SecretMetadata{}, "", ErrSecretUnavailable
	}
	var metadata SecretMetadata
	var ciphertext string
	var createdAt string
	var updatedAt string
	err := s.db.QueryRowContext(ctx, `SELECT id, kind, owner_id, ciphertext, key_version,
		created_at, updated_at FROM control_secrets WHERE kind = ? AND owner_id = ?`, kind, ownerID).Scan(
		&metadata.ID,
		&metadata.Kind,
		&metadata.OwnerID,
		&ciphertext,
		&metadata.KeyVersion,
		&createdAt,
		&updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SecretMetadata{}, "", ErrNotFound
	}
	if err != nil {
		return SecretMetadata{}, "", fmt.Errorf("read secret: %w", err)
	}
	metadata.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return SecretMetadata{}, "", ErrCorrupt
	}
	metadata.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return SecretMetadata{}, "", ErrCorrupt
	}
	plain, err := decryptSecret(s.secretKey, ciphertext)
	if err != nil {
		return SecretMetadata{}, "", fmt.Errorf("%w: secret decryption failed", ErrCorrupt)
	}
	return metadata, plain, nil
}

func encryptSecret(key []byte, plain string) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create secret cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create secret gcm: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate secret nonce: %w", err)
	}
	sealed := gcm.Seal(nil, nonce, []byte(plain), nil)
	tagSize := gcm.Overhead()
	ciphertext := sealed[:len(sealed)-tagSize]
	tag := sealed[len(sealed)-tagSize:]
	return secretPrefix + strings.Join([]string{
		base64.StdEncoding.EncodeToString(nonce),
		base64.StdEncoding.EncodeToString(tag),
		base64.StdEncoding.EncodeToString(ciphertext),
	}, ":"), nil
}

func decryptSecret(key []byte, value string) (string, error) {
	if !strings.HasPrefix(value, secretPrefix) {
		return "", errors.New("secret is not encrypted")
	}
	parts := strings.Split(strings.TrimPrefix(value, secretPrefix), ":")
	if len(parts) != 3 {
		return "", errors.New("invalid encrypted secret format")
	}
	nonce, err := base64.StdEncoding.DecodeString(parts[0])
	if err != nil {
		return "", errors.New("invalid secret nonce")
	}
	tag, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return "", errors.New("invalid secret tag")
	}
	ciphertext, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return "", errors.New("invalid secret ciphertext")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create secret cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create secret gcm: %w", err)
	}
	if len(nonce) != gcm.NonceSize() || len(tag) != gcm.Overhead() {
		return "", errors.New("invalid encrypted secret sizes")
	}
	sealed := append(ciphertext, tag...)
	plain, err := gcm.Open(nil, nonce, sealed, nil)
	if err != nil {
		return "", errors.New("secret authentication failed")
	}
	return string(plain), nil
}
