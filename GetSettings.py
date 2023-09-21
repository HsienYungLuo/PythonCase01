import json

class ConfigData:
    def __init__(Self, ConfigData):
        Self.ConfigData = ConfigData

    @staticmethod
    def GetConfigSettings(ConfigFile):
        
        try:
            with open(ConfigFile, 'r') as File:
                Configs = json.load(File)

                if Configs:
                    return ConfigData(Configs)
                else:
                    raise ValueError(" ！ 缺少設定資料。")
        except FileNotFoundError:
            raise FileNotFoundError(" ！ 找不到設定檔案。")
        except json.JSONDecodeError:
            raise ValueError(" ！ 設定檔案不是有效的JSON格式。")

class Model:
    def __init__(Self, Type):
        Self.Type = Type
    
    @staticmethod
    def GetModel(ConfigFile):
        #   取得檔案設定。
        Configs = ConfigData.GetConfigSettings(ConfigFile)

        #   取得 Model。
        ModelType = Configs.ConfigData["Modle"]

        #   檢查與回傳
        if ModelType:
            return Model(ModelType)
        else:
            raise ValueError(" ！ 缺少 Model設定。")

class Connection:
    def __init__(Self, Server, User, Password, Database):
        Self.Server = Server
        Self.User = User
        Self.Password = Password
        Self.Database = Database

    @staticmethod
    def GetDatabaseConnectionInfo(ConfigFile):
        #   取得檔案設定。
        Configs = ConfigData.GetConfigSettings(ConfigFile)

        #   取得 DatabaseConnectionSetting。
        ConnectionInfo = Configs.ConfigData["Connection"]        

        Server = ConnectionInfo["Server"]
        User = ConnectionInfo["User"]
        Password = ConnectionInfo["Password"]
        Database = ConnectionInfo["Database"]

        if Server and User and Password and Database:
                return Connection(Server, User, Password, Database)
        else:
            raise ValueError(" ！ 缺少 Connection設定。")