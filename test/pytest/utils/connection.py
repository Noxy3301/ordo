"""MySQL connection utility with environment variable support."""

import os
import mysql.connector


def get_connection(user: str = "root", password: str = "") -> mysql.connector.MySQLConnection:
    """Create MySQL connection using environment-aware socket path.
    
    Checks MYSQL_UNIX_PORT environment variable first, then falls back to defaults.
    """
    socket_path = os.environ.get("MYSQL_UNIX_PORT")
    
    connect_args = {
        "user": user,
        "password": password,
    }
    
    if socket_path:
        # Use explicit socket path from environment
        connect_args["unix_socket"] = socket_path
        print(f"[connection] Using socket: {socket_path}")
    else:
        # Fall back to 127.0.0.1:3307 via TCP
        connect_args["host"] = "127.0.0.1"
        connect_args["port"] = 3307
        print(f"[connection] Using host: 127.0.0.1:3307")
    
    return mysql.connector.connect(**connect_args)

