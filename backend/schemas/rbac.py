from enum import Enum


class UserRole(str, Enum):
    OWNER = "owner"
    ADMIN = "admin"
    CHRO = "chro"
    HR_MANAGER = "hr_manager"
    RECRUITER = "recruiter"
    VIEWER = "viewer"
    MEMBER = "member"
