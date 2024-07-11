_dev_mode = False


def set_dev_mode(dev_mode: bool):
    global _dev_mode
    _dev_mode = dev_mode


def get_dev_mode() -> bool:
    return _dev_mode
