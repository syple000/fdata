
def from_bytes_to_str(data: bytes) -> str:
    """
    将字节数据转换为十六进制字符串表示。
    
    :param data: 字节数据
    if :return: 十六进制字符串
    """
    if not data:
        return ""
    return data.hex()

def from_str_to_bytes(data: str) -> bytes:
    """
    将十六进制字符串转换为字节数据。
    
    :param data: 十六进制字符串
    :return: 字节数据
    """
    if not data:
        return b""
    try:
        return bytes.fromhex(data)
    except ValueError as e:
        raise ValueError(f"Invalid hex string: {data}") from e