package task;

import java.util.ResourceBundle;

public class Config {
    private static ResourceBundle bundle = ResourceBundle.getBundle("config");

    public static String str(ConfigEnum key) {
        return bundle.getString(key.name());
    }

    public static Integer num(ConfigEnum key) {
        return Integer.valueOf( bundle.getString(key.name()).replaceAll("_", "") );
    }

    public static void reload(String config) {
        bundle = ResourceBundle.getBundle(config);
    }

}
