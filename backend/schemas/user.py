from typing import Optional

from backend.schemas.rbac import UserRole
from pydantic import BaseModel, EmailStr, constr


class UserBase(BaseModel):
    firstname: str = constr(min_length=1, max_length=50)
    lastname: str = constr(min_length=1, max_length=50)
    email: EmailStr


class UserInvite(UserBase):
    role: UserRole
    job_role: str = ""
    phone: Optional[str] = constr(pattern=r"^\+[1-9]\d{0,2}\s\d{10}$")


class UserSignup(UserBase):
    password: str = constr(min_length=8, max_length=100)


class Token(BaseModel):
    access_token: str
    token_type: str


class EmailVerification(BaseModel):
    token: str
    user_id: str


class UserProfileFetch(BaseModel):
    access_token: Optional[str] = None
    company_id: Optional[str] = None
    user_id: Optional[str] = None
    secure_token: Optional[str] = None


class PasswordReset(BaseModel):
    password: str = constr(min_length=8, max_length=100)
    user_id: str
    mode: str
    secure_token: Optional[str] = None


class ForgotPassword(BaseModel):
    email: EmailStr
