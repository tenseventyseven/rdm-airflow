from utils.shell import execute_command
import subprocess


def test_execute_command_success() -> None:
    """Test that a simple shell command executes successfully."""
    return_code = execute_command("echo 'Hello World'")
    assert return_code == 0


def test_execute_command_failure() -> None:
    """Test that a failing shell command raises an exception."""
    try:
        execute_command("exit 1")
    except subprocess.CalledProcessError as e:
        assert e.returncode == 1
        assert "exit 1" in e.cmd
