import binascii
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5

def marki_encrypt(plain_text, modulus_hex):
    reversed_text = plain_text[::-1]
    e = 65537
    n = int(modulus_hex, 16)
    public_key = RSA.construct((n, e))
    cipher = PKCS1_v1_5.new(public_key)
    encrypted_data = cipher.encrypt(reversed_text.encode('utf-8'))
    return binascii.hexlify(encrypted_data).decode('utf-8')

# 网页给出的结果 (password123)
expected = "525c577e045a1e75c9d33d622c0086e7ed4f418657e712c1c63bece5c7f05fd17444bdee0a44a6aea7e8d2e0b3e39c0f4daf895c285d6cc84c0ef8e214cb5013f5d05a1110a07b54de67042e24906aa36f1becca6f03141bb9899715edbee6b4eb7efd8a205ef7fd897fdb6aed5c96ec336d6b17c4029e9169d97e1d3294e21b"
modulus = "b5f53d3e7ab166d99b91bdee1414364e97a5569d9a4da971dcf241e9aec4ee4ee7a27b203f278be7cc695207d19b9209f0e50a3ea367100e06ad635e4ccde6f8a7179d84b7b9b7365a6a7533a9909695f79f3f531ea3c329b7ede2cd9bb9722104e95c0f234f1a72222b0210579f6582fcaa9d8fa62c431a37d88a4899ebce3d"

actual = marki_encrypt("password123", modulus)
print(f"Expected: {expected}")
print(f"Actual:   {actual}")
print(f"Match:    {expected == actual}")
