from datetime import datetime, timedelta, timezone
from typing import Annotated, Union, Tuple, Literal

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
from jwt.exceptions import InvalidTokenError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.config import settings
from app.db.db_helper import db_helper
from app.db.models.users import User as DBUser

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/stats/token")

CREDENTIALS_EXCEPTION = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Could not validate credentials or token has expired",
    headers={"WWW-Authenticate": "Bearer"},
)

TOKEN_TYPE_ACCESS = "access"
TOKEN_TYPE_REFRESH = "refresh"


class JWTService:
    SECRET_KEY: str = settings.auth.secret_key
    ALGORITHM: str = settings.auth.algorithm
    ACCESS_TOKEN_EXPIRE_MINUTES: int = settings.auth.access_token_expire_minutes
    REFRESH_TOKEN_EXPIRE_DAYS: int = settings.auth.refresh_token_expire_days

    def create_access_token(
        self,
        data: dict,
        token_type: Literal[TOKEN_TYPE_ACCESS, TOKEN_TYPE_REFRESH],
        expires_delta: timedelta | None = None
    ) -> str:
        to_encode = data.copy()

        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta
        elif token_type == TOKEN_TYPE_ACCESS:
            expire = datetime.now(timezone.utc) + timedelta(minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES)
        elif token_type == TOKEN_TYPE_REFRESH:
            expire = datetime.now(timezone.utc) + timedelta(days=self.REFRESH_TOKEN_EXPIRE_DAYS)
        else:
            raise ValueError(f"Unknown token type: {token_type}")

        to_encode.update({
            "exp": expire,
            "sub": data.get("sub"),
            "token_type": token_type
        })

        encoded_jwt = jwt.encode(to_encode, self.SECRET_KEY, algorithm=self.ALGORITHM)
        return encoded_jwt

    def create_token_pair(self, data: dict) -> Tuple[str, str, int]:
        access_token_expires = timedelta(minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = self.create_access_token(
            data=data,
            token_type=TOKEN_TYPE_ACCESS,
            expires_delta=access_token_expires
        )

        refresh_token = self.create_access_token(
            data=data,
            token_type=TOKEN_TYPE_REFRESH
        )

        expires_in_seconds = int(access_token_expires.total_seconds())

        return access_token, refresh_token, expires_in_seconds

    @db_helper.connection
    async def get_user_from_db(self, username: str, *, session: AsyncSession) -> Union[DBUser, None]:
        stmt = select(DBUser).where(DBUser.username == username)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    def decode_token(
        self,
        token: str,
        expected_type: Literal[TOKEN_TYPE_ACCESS, TOKEN_TYPE_REFRESH]
    ) -> str:
        try:
            payload = jwt.decode(token, self.SECRET_KEY, algorithms=[self.ALGORITHM])

            username: str | None = payload.get("sub")
            token_type: str | None = payload.get("token_type")

            if username is None or token_type != expected_type:
                raise CREDENTIALS_EXCEPTION

            return username
        except (InvalidTokenError, JWTError):
            raise CREDENTIALS_EXCEPTION


jwt_service = JWTService()


async def get_current_user(
    token: Annotated[str, Depends(oauth2_scheme)]
) -> DBUser:
    username = jwt_service.decode_token(token, expected_type=TOKEN_TYPE_ACCESS)
    user = await jwt_service.get_user_from_db(username=username)

    if user is None:
        raise CREDENTIALS_EXCEPTION

    return user


async def get_current_refresh_user(
    refresh_token: Annotated[str, Depends(oauth2_scheme)]
) -> DBUser:
    username = jwt_service.decode_token(refresh_token, expected_type=TOKEN_TYPE_REFRESH)
    user = await jwt_service.get_user_from_db(username=username)

    if user is None:
        raise CREDENTIALS_EXCEPTION

    return user
