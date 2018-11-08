
import sys
import socket


def test_socket(socket_host, socket_port, service_name):
    """Test socket connectivity to requested service port

        Args:
            socket_host (str): Host name to test connectivity to
            socket_port (str): Port number for service
            service_name (str): Readable name for the service
        Returns:
            bool: Dremio Response status. True for success, False otherwise.
    """

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((socket_host, int(socket_port)))
    except Exception as e:
        print(
            "Unable to connect to %s host %s:%d. Exception is %s" %
            (service_name, socket_host, int(socket_port), e))
        return False
    finally:
        s.close()
        return True
