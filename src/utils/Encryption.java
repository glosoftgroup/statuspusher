 package com.cellulant.statusPusher.utils;


import java.io.UnsupportedEncodingException;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Encryption class using AES 128 bit with CBC and PKCS5Padding.
 *
 * @author Brian Ngure
 */
@SuppressWarnings("FinalClass")
public final class Encryption {
    /**
     * Initialisation vector string.
     */
    private String iv;
    /**
     * Initialisation vector instance.
     */
    private IvParameterSpec ivspec;
    /**
     * The secret key string.
     */
    private String secretKey;
    /**
     * The secret key specification class instance.
     */
    private SecretKeySpec keyspec;
    /**
     * The cryptographic cipher for encryption and decryption.
     */
    private Cipher cipher;


    /**
     * Constructor.
     *
     * @param initialisationVector the initialisation vector
     * @param key the secret key
     *
     * @throws NoSuchAlgorithmException if the algorithm is not found
     * @throws NoSuchPaddingException if the padding is not correct
     */
    public Encryption(final String initialisationVector, final String key)
            throws NoSuchAlgorithmException, NoSuchPaddingException {
        if (initialisationVector == null) {
            throw new IllegalArgumentException("Initialisation vector must not "
                    + "be null");
        } else if (initialisationVector.length() != 16) {
            throw new IllegalArgumentException("Initialisation vector must be "
                    + "16 characters in length");
        } else {
            iv = initialisationVector;
        }

        if (key == null) {
            throw new IllegalArgumentException("Secret key must not be null");
        } else if (key.length() != 16) {
            throw new IllegalArgumentException("Secret key must be 16 "
                    + "characters in length");
        } else {
            secretKey = key;
        }

        ivspec = new IvParameterSpec(iv.getBytes());

        keyspec = new SecretKeySpec(secretKey.getBytes(), "AES");

        cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
    }

    /**
     * Encrypts the given string.
     *
     * @param text the text to encrypt
     *
     * @return text encrypted text as a byte array
     *
     * @throws InvalidKeyException on error
     * @throws IllegalBlockSizeException on error
     * @throws InvalidAlgorithmParameterException on error
     * @throws BadPaddingException on error
     * @throws UnsupportedEncodingException on error
     */
    public byte[] encrypt(final String text)
            throws InvalidKeyException, IllegalBlockSizeException,
            InvalidAlgorithmParameterException, BadPaddingException,
            UnsupportedEncodingException {
        if (text == null || text.length() == 0) {
            throw new IllegalArgumentException("Empty string");
        }

        cipher.init(Cipher.ENCRYPT_MODE, keyspec, ivspec);

        return cipher.doFinal(text.getBytes("UTF-8"));
    }

    /**
     * Decrypts the given string.
     *
     * @param code the text to decrypt
     *
     * @return text decrypted text as a byte array
     *
     * @throws InvalidKeyException on error
     * @throws IllegalBlockSizeException on error
     * @throws InvalidAlgorithmParameterException on error
     * @throws BadPaddingException on error
     */
    public byte[] decrypt(final String code)
            throws InvalidKeyException, InvalidAlgorithmParameterException,
            IllegalBlockSizeException, BadPaddingException {
        if (code == null || code.length() == 0) {
            throw new IllegalArgumentException("Empty string");
        }

        cipher.init(Cipher.DECRYPT_MODE, keyspec, ivspec);

        return cipher.doFinal(hexToBytes(code));
    }

    /**
     * Converts a byte array to a hex string.
     *
     * @param data the byte array
     *
     * @return the hex string
     */
    public static String bytesToHex(final byte[] data) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Empty byte array");
        }

        int len = data.length;

        StringBuilder sb = new StringBuilder(2);
        for (int i = 0; i < len; i++) {
            if ((data[i] & 0xFF) < 16) {
                sb.append("0").append(Integer.toHexString(data[i] & 0xFF));
            } else {
                sb.append(Integer.toHexString(data[i] & 0xFF));
            }
        }

        return sb.toString();
    }

    /**
     * Converts a hex string to a byte array.
     *
     * @param str the hex string
     *
     * @return the byte array
     */
    public static byte[] hexToBytes(final String str) {
        if (str == null) {
            throw new IllegalArgumentException("Empty string");
        } else if (str.length() < 2) {
            throw new IllegalArgumentException("Invalid hex string");
        } else {
            int len = str.length() / 2;
            byte[] buffer = new byte[len];

            for (int i = 0; i < len; i++) {
                buffer[i] = (byte) Integer
                        .parseInt(str.substring(i * 2, i * 2 + 2), 16);
            }

            return buffer;
        }
    }
} 