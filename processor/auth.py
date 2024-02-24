from typing import Generator
from contextlib import contextmanager
from datetime import datetime

from supabase import Client

from .utils.settings import settings
from .utils.supabase_client import use_client, login

__ACCESS_TOKEN = None
__REFRESH_TOKEN = None
__ISSUED_AT = None
__USER_ID = None


def authenticate_processor():
    # use the global TOKENS
    global __ACCESS_TOKEN
    global __REFRESH_TOKEN
    global __ISSUED_AT
    
    # if there is no access token
    if __ACCESS_TOKEN is None or __REFRESH_TOKEN is None:    
        # login to the supabase backend to retrieve an access token
        auth = login(settings.processor_username, settings.processor_password)

        # upadate
        __ACCESS_TOKEN = auth.session.access_token
        __REFRESH_TOKEN = auth.session.refresh_token
        __ISSUED_AT = datetime.now()
        __USER_ID = auth.user.id

    # check if we need to refresh the token
    elif (datetime.now() - __ISSUED_AT).total_seconds() > 60 * 45:  # 45 minutes
        with use_client(__ACCESS_TOKEN) as client:
            auth = client.auth.refresh_session(__REFRESH_TOKEN)

            # update the tokens
            __ACCESS_TOKEN = auth.session.access_token
            __REFRESH_TOKEN = auth.session.refresh_token
            __ISSUED_AT = datetime.now()
            __USER_ID = auth.user.id
    
    else:
        # there is nothing to do
        pass


@contextmanager
def supabase_client() -> Generator[Client, None, None]:
    # authenticate the processor
    authenticate_processor()

    # create a supabase client
    with use_client(__ACCESS_TOKEN) as client:
        yield client


def get_user_id() -> str:
    if __USER_ID is None:
        authenticate_processor()
    
    return __USER_ID
