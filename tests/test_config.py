import unittest

from src.config import PluginSettings


class PluginSettingsTest(unittest.TestCase):
    def test_whitelist_matches_plain_group_id(self):
        settings = PluginSettings(
            group_list_mode="whitelist",
            group_list=["123456"],
        )
        self.assertTrue(
            settings.matches_group(
                platform_id="onebot",
                group_id="123456",
                unified_msg_origin="onebot:GroupMessage:123456",
                session_id="123456",
            )
        )

    def test_whitelist_matches_unified_msg_origin(self):
        settings = PluginSettings(
            group_list_mode="whitelist",
            group_list=["napcat_main:GroupMessage:456789"],
        )
        self.assertTrue(
            settings.matches_group(
                platform_id="napcat_main",
                group_id="456789",
                unified_msg_origin="napcat_main:GroupMessage:456789",
                session_id="456789",
            )
        )

    def test_blacklist_blocks_group(self):
        settings = PluginSettings(
            group_list_mode="blacklist",
            group_list=["456789"],
        )
        self.assertFalse(
            settings.matches_group(
                platform_id="onebot",
                group_id="456789",
                unified_msg_origin="onebot:GroupMessage:456789",
                session_id="456789",
            )
        )

    def test_disabled_plugin_never_matches(self):
        settings = PluginSettings(enabled=False)
        self.assertFalse(
            settings.matches_group(
                platform_id="onebot",
                group_id="123456",
                unified_msg_origin="onebot:GroupMessage:123456",
                session_id="123456",
            )
        )


if __name__ == "__main__":
    unittest.main()
