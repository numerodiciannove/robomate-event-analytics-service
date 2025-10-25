import bcrypt
from typing import Union, Tuple
from loguru import logger

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.db.db_helper import db_helper
from app.schemas.users import UserCreate
from app.db.models.users import User as DBUser
from app.services.jwt_service import jwt_service


def verify_password(plain_password: str, hashed_password: str) -> bool:
    is_valid = bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))
    logger.debug(f"Password verification result: {is_valid}")
    return is_valid


def get_password_hash(password: str) -> str:
    hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    logger.debug(f"Generated password hash: {hashed[:10]}...")
    return hashed


class AuthService:

    @db_helper.connection
    async def get_user_if_exists(self, username: str, *, session: AsyncSession) -> Union[DBUser, None]:
        logger.debug(f"Checking if user exists: {username}")
        stmt = select(DBUser).where(DBUser.username == username)
        result = await session.execute(stmt)
        user = result.scalar_one_or_none()
        logger.debug(f"User found: {user.username if user else None}")
        return user

    @db_helper.connection
    async def create_user_in_db(self, user: UserCreate, *, session: AsyncSession) -> DBUser:
        logger.info(f"Creating user in DB: {user.username}")
        hashed_password = get_password_hash(user.password)
        db_user = DBUser(username=user.username, hashed_password=hashed_password)
        session.add(db_user)
        await session.commit()
        logger.info(f"User created: {db_user.username} (ID: {db_user.id})")
        return db_user

    async def register_user(self, user_data: UserCreate) -> DBUser:
        logger.debug(f"Registering user: {user_data.username}")
        return await self.create_user_in_db(user_data)

    async def authenticate_user(self, username: str, password: str) -> Union[DBUser, None]:
        logger.debug(f"Authenticating user: {username}")
        user = await self.get_user_if_exists(username=username)

        if not user:
            logger.warning(f"Authentication failed: user {username} not found")
            return None

        if not verify_password(password, user.hashed_password):
            logger.warning(f"Authentication failed: invalid password for user {username}")
            return None

        logger.info(f"User authenticated: {username}")
        return user

    @staticmethod
    def create_token_for_user(user: DBUser) -> Tuple[str, str, int]:
        logger.debug(f"Creating token pair for user: {user.username}")
        data = {"sub": user.username}
        access_token, refresh_token, expires_in_seconds = jwt_service.create_token_pair(data)
        logger.info(f"Token pair created for user: {user.username}, expires in {expires_in_seconds} sec")
        return access_token, refresh_token, expires_in_seconds


auth_service = AuthService()
