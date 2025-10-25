from pydantic import EmailStr, BaseModel


class Token(BaseModel):
    """Model for the response after successful authentication or token refresh."""
    access_token: str
    token_type: str = "bearer"
    refresh_token: str
    expires_in: int


class TokenData(BaseModel):
    """Data extracted from the token (e.g., for validation)."""
    username: str | None = None


class UserBase(BaseModel):
    """Base user schema."""
    username: str
    email: EmailStr | None = None
    full_name: str | None = None


class UserCreate(UserBase):
    """Schema for registering a new user."""
    password: str


class User(UserBase):
    """User schema returned to the client (without password)."""
    id: int
    disabled: bool | None = None

    class Config:
        from_attributes = True
