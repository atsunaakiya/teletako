import os

from pixivpy3 import AppPixivAPI

ACCESS_TOKEN_FILE = 'cache/pixiv_refresh_token.txt'

def login(api: AppPixivAPI):
    username = 'viff_automator@yahoo.co.jp'
    password = '93jE!L#7mTHhAB'
    if os.path.exists(ACCESS_TOKEN_FILE):
        with open(ACCESS_TOKEN_FILE) as f:
            token = f.read()
        api.auth(username=username, refresh_token=token)
    else:
        api.auth(username=username, password=password)
    with open(ACCESS_TOKEN_FILE, 'w') as f:
        f.write(api.refresh_token)

def main():
    api = AppPixivAPI()
    login(api)
    print(api.user_bookmarks_illust(4800296))

if __name__ == '__main__':
    main()
