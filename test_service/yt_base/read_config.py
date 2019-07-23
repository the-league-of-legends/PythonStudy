import configparser
import codecs

class ReadConfig:
    """
    专门读取配置文件的，.ini文件格式
    """
    def __init__(self, config_path):
        fd = open(config_path)
        data = fd.read()

        # remove BOM
        if data[:3] == codecs.BOM_UTF8:
            data = data[3:]
            files = codecs.open(config_path, "w")
            files.write(data)
            files.close()
        fd.close()

        self.cf = configparser.ConfigParser()
        self.cf.read(config_path)

    def get_value(self, env, name):
        """
        [projectConfig]
        project_path=E:/Python-Project/UItestframework
        :param env:[projectConfig]
        :param name:project_path
        :return:E:/Python-Project/UItestframework
        """
        return self.cf.get(env, name)

    def set_value(self, env, name, value):
        self.cf.set(env, name, value)

