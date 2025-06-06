from typing import Tuple

from app.schemas.rbac import UserRole
from typing_extensions import TypedDict


class UserInfo(TypedDict):
    user_id: str
    email: str
    role: str


UserData = Tuple[UserInfo, UserRole]
