from typing import Annotated
from fastapi import Depends, HTTPException, status, APIRouter
from fastapi.security import OAuth2PasswordRequestForm
from loguru import logger

from app.db.models.users import User as DBUser
from app.services.auth_service import auth_service
from app.services.jwt_service import get_current_user, get_current_refresh_user
from app.schemas.users import UserCreate, User, Token

user_router = APIRouter(prefix="/stats")


@user_router.post("/register", response_model=User, status_code=status.HTTP_201_CREATED)
async def register_user_endpoint(user_data: UserCreate):
    try:
        existing_user = await auth_service.get_user_if_exists(username=user_data.username)
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="A user with this username already exists"
            )

        db_user = await auth_service.register_user(user_data)
        return User.model_validate(db_user)

    except HTTPException:
        raise  # handled by FastAPI
    except Exception as e:
        logger.exception(f"Failed to register user: {user_data.username}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error occurred while registering the user"
        ) from e


@user_router.post("/token", response_model=Token)
async def login_for_access_token(
        form_data: Annotated[OAuth2PasswordRequestForm, Depends()]
):
    try:
        user = await auth_service.authenticate_user(form_data.username, form_data.password)

        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )

        access_token, refresh_token, expires_in = auth_service.create_token_for_user(user)

        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_in": expires_in
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to login user: {form_data.username}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error occurred during authentication"
        ) from e


@user_router.get("/users/me", response_model=User)
async def read_users_me(
        current_user: Annotated[DBUser, Depends(get_current_user)]
):
    try:
        return User.model_validate(current_user)
    except Exception as e:
        logger.exception(f"Failed to fetch current user info: {current_user.username}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error occurred while fetching user information"
        ) from e
