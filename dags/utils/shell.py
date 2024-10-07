import subprocess
import logging


def execute_command(command: str, logger: logging.Logger | None = None):
    """
    Executes a shell command, streaming stdout in real time.
    Handles errors by logging stderr.

    Parameters:
        command: The shell command to execute.
        logger: A configured logger for logging output and errors.
                If not provided, falls back to using print.

    Returns:
        int: The return code of the executed command.

    Raises:
        subprocess.CalledProcessError: If the command fails (non-zero return code).
    """
    if logger is None:
        # Set up the default basic logger if not already configured
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

    try:
        logger.info(f"Running command: {command}")
        process = subprocess.Popen(
            command,
            shell=True,  # Dangerous! See https://docs.python.org/3/library/subprocess.html#security-considerations
            text=True,
            bufsize=1,  # Line buffered
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Stream stdout in real time
        if process.stdout:
            for line in iter(process.stdout.readline, ""):
                logger.info(line.strip())

        # Wait for the process to complete
        process.wait()

        # Check for errors after process completes
        if process.stderr and process.returncode != 0:
            stderr_output = process.stderr.read()
            logger.error(f"Command failed with return code {process.returncode}")
            logger.error(f"Stderr output:\n{stderr_output}")
            raise subprocess.CalledProcessError(
                process.returncode, command, stderr_output
            )

        logger.info("Command completed successfully.")
        return process.returncode

    except subprocess.CalledProcessError as e:
        # Log the error and re-raise the exception
        logger.error(f"Error: Command failed with return code {e.returncode}")
        logger.error(f"Command: {e.cmd}")
        logger.error(f"Stderr output:\n{e.stderr}")
        raise
