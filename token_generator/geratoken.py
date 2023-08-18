import secrets
import string


def generate_token(length=32):
    alphabet = string.ascii_letters + string.digits
    token = ''.join(secrets.choice(alphabet) for _ in range(length))
    return token


# Generate a token of length 32
tk = generate_token()
print("Generated Token:", tk)
