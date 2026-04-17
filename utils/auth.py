def hash_password(password: str) -> str:
    from argon2 import PasswordHasher
    ph = PasswordHasher()
    return ph.hash(password)

def verify_password(password: str, hashed_password: str) -> bool:
    from argon2 import PasswordHasher
    from argon2.exceptions import VerifyMismatchError
    ph = PasswordHasher()
    try:
        ph.verify(hashed_password, password)
        return True
    except VerifyMismatchError:
        return False