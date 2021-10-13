import os
import time

import config


def main():
    while True:
        try:
            if (
                time.time() - os.path.getmtime(config.HEARTBEAT_PATH)
                > config.HEARTBEAT_INTERVAL * 2.2
            ):
                print("restarting service")
                os.system(f"net stop {config.SERVICE_NAME} & net start {config.SERVICE_NAME}")
            else:
                print("pass")
        except OSError:
            print("heartbeat path not found")
        time.sleep(config.HEARTBEAT_INTERVAL * 1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt as e:
        print("KeyboardInterrupt")
