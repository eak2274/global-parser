# utils/header_provider.py

from browserforge.headers import HeaderGenerator

class HeaderProvider:
    """
    Provides a complete and highly realistic set of browser headers
    using the browserforge library.
    """
    def __init__(self):
        # Use HeaderGenerator
        self.generator = HeaderGenerator()

    def get_random_header(self) -> dict[str, str]:
        """
        Generates and returns a complete set of random browser headers.
        """
        # Call the generate() method on our instance
        return self.generator.generate()