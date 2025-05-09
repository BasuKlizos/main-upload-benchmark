from typing import List, Optional, Union

from app.api.utils import api_key_ops
from app.schemas.rbac import UserRole
from app.security.auth import get_current_user_role
from app.security.perms import *
from app.types_ import UserData
from fastapi import Depends, Header, HTTPException, Request, status

class PermissionChecker:
    """
    Dependency class for checking user permissions.

    This class can be used as a FastAPI dependency to enforce that the current user
    has the required set of permissions before accessing an endpoint.
    """

    def __init__(self, required_permissions: List[Permission]):
        """
        Initialize the PermissionChecker with the required permissions.

        Args:
            required_permissions (List[Permission]): A list of permissions required to access the endpoint.
        """
        self.required_permissions = required_permissions

    async def __call__(
        self,
        user_role: UserData = Depends(get_current_user_role),
    ) -> UserData:
        """
        Callable method that checks if the user has the required permissions.

        Args:
            user_role (UserData): The user data and role of the current user, injected by FastAPI's dependency injection.

        Raises:
            HTTPException: If the user does not have the required permissions.

        Returns:
            UserData: The user data if permissions are sufficient.
        """
        if not self.has_permissions(user_role[1]):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have the required permissions",
            )
        return user_role

    def has_permissions(self, role: UserRole) -> bool:
        """
        Check if the given role includes all required permissions.

        Args:
            role (UserRole): The role of the user.

        Returns:
            bool: True if the role has all required permissions, False otherwise.
        """
        user_permissions = ROLE_PERMISSIONS.get(role, set())
        return all(perm in user_permissions for perm in self.required_permissions)

def get_user_details_factory(required_permissions: List[Permission]):
    """
    Factory function that creates a dependency for authenticating users via API key or bearer token.

    This factory creates an async dependency function that checks authentication credentials
    and validates permissions. It supports two authentication methods:
    1. API Key authentication via X-API-Key header
    2. Bearer token authentication via Authorization header

    Args:
        required_permissions (List[Permission]): List of permissions required to access the endpoint

    Returns:
        Depends: FastAPI dependency that handles authentication and permission checking
    """

    # Create permission checker once at factory level rather than per request
    checker = PermissionChecker(required_permissions)

    async def get_api_key_data(
        request: Request,
        x_api_key: Optional[str] = Header(None),
        authorization: Optional[str] = Header(None),
    ) -> Union[UserData, dict]:
        """
        Authenticate and authorize user via API key or bearer token.

        Args:
            request (Request): FastAPI request object
            x_api_key (Optional[str]): API key from X-API-Key header
            authorization (Optional[str]): Bearer token from Authorization header

        Returns:
            Union[UserData, dict]: User data and role if authenticated

        Raises:
            HTTPException: If authentication fails or permissions are insufficient
        """
        # Check API key authentication first
        if x_api_key:
            return await api_key_ops(mode="get", x_api_key=x_api_key)

        # Check bearer token authentication
        if authorization and authorization.startswith("Bearer "):
            token = authorization.removeprefix("Bearer ").strip()
            try:
                user_role = await get_current_user_role(token=token)
                return await checker(user_role)
            except HTTPException:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid bearer token",
                )

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No valid authentication credentials provided",
        )

    return Depends(get_api_key_data)
