package security

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"golang.org/x/crypto/hkdf"
	"io"
)

const (
	nonceSize = 12 // 96-bit nonce recommended for AES-GCM
	tagSize   = 16 // 128-bit authentication tag
	keySize   = 32 // AES-256
)

// Decrypt credential payload that was encrypted by the C# AesGcmEncryptorBase.
// Equivalent to AesGcmEncryptorBase.Decrypt(string ciphertext)
// masterKeyBase64: A 32-byte (256-bit) Base64-encoded master key from configuration.
// credentialType: The string representation of CredentialType enum (e.g., "DatabasePassword").
func Decrypt(ciphertextBase64 string, credentialType string, masterKeyBase64 string) (string, error) {
	if ciphertextBase64 == "" {
		return "", errors.New("ciphertext cannot be empty")
	}

	masterKeyBytes, err := base64.StdEncoding.DecodeString(masterKeyBase64)
	if err != nil {
		return "", fmt.Errorf("failed to decode master key: %w", err)
	}

	if len(masterKeyBytes) != keySize {
		return "", fmt.Errorf("invalid master key length, expected %d bytes but got %d", keySize, len(masterKeyBytes))
	}

	// Derive the same sub-key as C# via HKDF-SHA256
	info := []byte(credentialType)
	derivedKey := make([]byte, keySize)
	hkdfReader := hkdf.New(sha256.New, masterKeyBytes, nil, info)
	if _, err := io.ReadFull(hkdfReader, derivedKey); err != nil {
		return "", fmt.Errorf("failed to derive sub-key: %w", err)
	}

	packed, err := base64.StdEncoding.DecodeString(ciphertextBase64)
	if err != nil {
		return "", fmt.Errorf("failed to decode ciphertext base64: %w", err)
	}

	if len(packed) < nonceSize+tagSize {
		return "", errors.New("ciphertext is too short to be valid")
	}

	// Unpack: [ nonce (12 bytes) | tag (16 bytes) | ciphertext (N bytes) ]
	nonce := packed[:nonceSize]
	tag := packed[nonceSize : nonceSize+tagSize]
	encryptedBytes := packed[nonceSize+tagSize:]

	block, err := aes.NewCipher(derivedKey)
	if err != nil {
		return "", fmt.Errorf("failed to create AES cipher: %w", err)
	}

	aesGcm, err := cipher.NewGCMWithNonceSize(block, nonceSize)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM block: %w", err)
	}

	// In Go crypto/cipher, the standard Seal/Open expects the tag to be appended to the ciphertext.
	// Our C# explicitly places the tag BEFORE the ciphertext: [nonce|tag|ciphertext].
	// To make Go's Open work, we need to pass a slice where the tag is AT THE END.
	// We'll create a new slice with the standard format: [ciphertext|tag]
	goFormatCiphertext := make([]byte, 0, len(encryptedBytes)+len(tag))
	goFormatCiphertext = append(goFormatCiphertext, encryptedBytes...)
	goFormatCiphertext = append(goFormatCiphertext, tag...)

	plaintext, err := aesGcm.Open(nil, nonce, goFormatCiphertext, nil)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt ciphertext: %w", err)
	}

	return string(plaintext), nil
}
