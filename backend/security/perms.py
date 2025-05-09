from enum import Enum
from typing import Dict, Set

from backend.schemas.rbac import UserRole

__all__ = ["Permission", "ROLE_PERMISSIONS"]


class Permission(str, Enum):
    """
    Enum class defining all available permissions in the system.
    Each permission represents a specific action that can be performed.
    """

    MANAGE_USERS = "manage_users"  # Permission to manage user accounts
    ASSIGN_ROLES = "assign_roles"  # Permission to assign roles to users
    MANAGE_SETTINGS = "manage_settings"  # Permission to modify system settings
    BULK_DOWNLOAD = "bulk_download"  # Permission to download data in bulk
    MANAGE_CANDIDATES = "manage_candidates"  # Permission to manage candidate profiles
    EDIT_CANDIDATES = "edit_candidates"  # Permission to edit candidate information
    VIEW_CANDIDATES = "view_candidates"  # Permission to view candidate profiles
    SCHEDULE_INTERVIEWS = "schedule_interviews"  # Permission to schedule interviews
    DELETE_USERS = "delete_users"  # Permission to delete user accounts
    JOB_OPERATIONS = "job_operations"  # Permission to perform job operations
    GET_USAGE_COST_USER = "get_cost_user"  # Permisson to get llm cost by user usage
    GET_USAGE_COST_COMPANY = "get_cost_company"  # Permisson to get the total llm cost by company usage


# Define role-based permissions using a type-safe dictionary
ROLE_PERMISSIONS: Dict[UserRole, Set[Permission]] = {
    UserRole.OWNER: {
        Permission.MANAGE_USERS,
        Permission.ASSIGN_ROLES,
        Permission.MANAGE_SETTINGS,
        Permission.BULK_DOWNLOAD,
        Permission.MANAGE_CANDIDATES,
        Permission.EDIT_CANDIDATES,
        Permission.VIEW_CANDIDATES,
        Permission.SCHEDULE_INTERVIEWS,
        Permission.DELETE_USERS,
        Permission.JOB_OPERATIONS,
        Permission.GET_USAGE_COST_USER,
        Permission.GET_USAGE_COST_COMPANY,
    },
    UserRole.ADMIN: {
        Permission.MANAGE_USERS,
        Permission.ASSIGN_ROLES,
        Permission.MANAGE_SETTINGS,
        Permission.BULK_DOWNLOAD,
        Permission.MANAGE_CANDIDATES,
        Permission.EDIT_CANDIDATES,
        Permission.VIEW_CANDIDATES,
        Permission.SCHEDULE_INTERVIEWS,
        Permission.JOB_OPERATIONS,
        Permission.GET_USAGE_COST_USER,
        Permission.GET_USAGE_COST_COMPANY,
    },
    UserRole.CHRO: {
        Permission.BULK_DOWNLOAD,
        Permission.MANAGE_CANDIDATES,
        Permission.EDIT_CANDIDATES,
        Permission.VIEW_CANDIDATES,
        Permission.SCHEDULE_INTERVIEWS,
        Permission.GET_USAGE_COST_USER,
        Permission.GET_USAGE_COST_COMPANY,
    },
    UserRole.HR_MANAGER: {
        Permission.MANAGE_CANDIDATES,
        Permission.EDIT_CANDIDATES,
        Permission.VIEW_CANDIDATES,
        Permission.SCHEDULE_INTERVIEWS,
        Permission.BULK_DOWNLOAD,
        Permission.GET_USAGE_COST_USER,
    },
    UserRole.RECRUITER: {
        Permission.EDIT_CANDIDATES,
        Permission.VIEW_CANDIDATES,
        Permission.SCHEDULE_INTERVIEWS,
        Permission.GET_USAGE_COST_USER,
    },
    UserRole.VIEWER: {Permission.VIEW_CANDIDATES},
    UserRole.MEMBER: {Permission.VIEW_CANDIDATES},
}
