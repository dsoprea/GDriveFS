from unittest import TestCase, main

from gdrivefs.gdtool import Conf

class ConfTestCase(TestCase):
    """Test the Conf class."""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_config(self):
        """Test for the existence of all configuration keys."""

        keys = [ 'auth_temp_path',
                 'auth_cache_filename',
                 'auth_secrets_filepath',
                 'change_check_interval_s']
        try:
            for key in keys:
                Conf.get(key)
        except (Exception) as e:
            self.fail("Could not retrieve value for configuration key [%s]." % 
                      (key))

if __name__ == '__main__':
    main()

