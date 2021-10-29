import os
import glob
import sqlite3
import pandas as pd
import plistlib
import re
import shutil
import sys
from parse3 import *
import datetime
import ntpath
import filetype
import ccl_bplist
from pathlib import Path
from platform import system

def proto_to_msg(bin_file):
    messages_found = []
    messages = ParseProto(bin_file)

    res = find_string_in_dict(messages)
    for k, v in res:
        if "string" in k:
            messages_found.append(v)

    return messages_found

def find_string_in_dict(data):
    for k, v in data.items():
        if isinstance(v, dict):
            yield k, v
            yield from find_string_in_dict(v)
        else:
            yield k, v

def path_to_image_html(filename):
    global attachmentPath_relative
    global exe_path
    global outputDir_name

    path = Path(outputDir + "/cacheFiles/" + filename)
    try:
        path = path.replace("\\", "/")
    except Exception:
        pass
    if os.path.exists(path):
        try:
            basename = ntpath.basename(path)
            realpath = os.path.abspath(path)
            kind = filetype.guess(path)
            if platform == "Windows":
                relpath = realpath.split("\\")[-2:]
            else:
                relpath = realpath.split("/")[-2:]
            relpath = str(Path(relpath[0]+"/"+relpath[1]))
            if platform == "Windows":
                if kind.extension == "mp4":
                    return ('<video width="320" height="240" controls> <source src="' + (relpath) + '" type="video/mp4"> Your browser does not support the video tag. </video> <a href="'+str(relpath)+'"><br>'+basename+'</a>')
                elif kind.extension == "png":
                    return ('<a href="' + str(relpath) + '"><img src="' + str(relpath) + '" width="150" ><br>'+basename+'</a>')
                elif kind.extension == "jpg":
                    return ('<a href="' + str(relpath) + '"><img src="' + str(relpath) + '" width="150" ><br>'+basename+'</a>')
                else:
                    return filename + " - Unknown extension: " + kind.extension
            else:
                if kind.extension == "mp4":
                    return ('<video width="320" height="240" controls> <source src="' + (relpath) + '" type="video/mp4"> Your browser does not support the video tag. </video> <a href="'+(relpath)+'"><br>'+basename+'</a>')
                elif kind.extension == "png":
                    return ('<a href="' + (relpath) + '"><img src="' + (relpath) + '" width="150" ><br>'+basename+'</a>')
                elif kind.extension == "jpg":
                    return ('<a href="' + (relpath) + '"><img src="' + (relpath) + '" width="150" ><br>'+basename+'</a>')
                else:
                    return filename + " - Unknown extension: " + kind.extension
            
        except Exception as Error:
            print(Error)

            return filename + " missing attachment"
        
    else:
        return filename

def getUserID(userPlist):
    print("Getting User ID from " + ntpath.basename(userPlist))
    with open(userPlist, "rb") as f:
        data = f.read()
        uuid = re.search('[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', str(data))

    print("")    
    return(uuid.group(0))

def getFriendsPlist(group_plist):
    print("Getting friends and groups from " + ntpath.basename(group_plist))
    try:
        with open(group_plist, "rb")as f:
            data = plistlib.loads(f.read())
    
        with open("test.plist", "wb") as test:
            try:
                test.write(data["share_user"])
            except KeyError:
                print("Can not find key 'share_user' in plist, getting friends from primary.docobjects")
                try:
                    test.write(data["user"])
                    print("Found key 'user' in plist, this is an older version of storing friends not yet supported by this script")
                except:
                    pass
                
                return
            
        with open("test.plist", "rb") as test:
            plist = ccl_bplist.load(test)
            data1 = ccl_bplist.deserialise_NsKeyedArchiver(plist)
            
        os.remove("test.plist")

        display = []
        name = []
        conversation_id = []
        group_participants = []
        user_id = []
        group_name = ""
        groups = {"Group Name":[], "Participants":[], "Conversation ID":[]}
        df_group = pd.DataFrame(columns=['Group Name', 'Participants', 'Conversation ID'])

        
        try:
            current_username = "<b>" + data1["USERNAME"] + "</b>"
            current_userid = data1["USER_ID"]
            name.append(current_username)
            user_id.append(current_userid)
            conversation_id.append("")
            display.append("")
        except:
            print("Could not find logged in user info")
        
        try:
            for i in data1["SECTIONS"]["NS.objects"]:
                friends = i["DESTINATIONS"]["NS.objects"]
                for i in friends:
                    try:
                        user_id.append(i["FRIEND_BITMOJI_INFO"]["USER_ID"])
                        tmp_display = i["FRIEND_DISPLAY"].encode('cp1252', 'xmlcharrefreplace')
                        tmp_display = tmp_display.decode('cp1252')
                        display.append(tmp_display)
                        conversation_id.append(i["FRIEND_CONVERSATION_ID"]["NS.string"])
                        name.append(i["FRIEND_NAME"])
                    except Exception as Error:
                        try:
                            try:
                                group_name = i['GROUP_GROUP_NAME']["NS.string"].encode('cp1252', 'xmlcharrefreplace').decode('cp1252')
                                group_id = i['GROUP_GROUP_ID']
                            except:
                                try:
                                    group_name = i['GROUP_GROUP_NAME'].encode('cp1252', 'xmlcharrefreplace').decode('cp1252')
                                    group_id = i['GROUP_GROUP_ID']
                                except Exception as Error:
                                    print(Error)
                            group_participants = []
                            try:
                                if i['GROUP_GROUP_PARTICIPANTS_USER_NAMES'] != "$null":
                                    for j in i['GROUP_GROUP_PARTICIPANTS_USER_NAMES']['NS.objects']:
                                        group_participants.append(str(j))
                                
                            except Exception as Error:
                                print(Error, "Error")
                                os.system("pause")
                            if len(group_participants) >= 1:
                                groups["Conversation ID"].append(group_id)
                                groups["Group Name"].append(group_name)
                                groups["Participants"].append(group_participants)
                            else:
                                pass
                        except Exception as Error:
                            print(Error)
                            pass
        except Exception as Error:
            print(Error)
    except Exception as Error:
        print(Error)
        return
    resultat = {'Display name': display, 'Username': name, 'User ID': user_id, 'Conversation ID': conversation_id} 
    df_friends = pd.DataFrame(resultat)
    df_group = pd.DataFrame(groups)

    df_friends = df_friends.drop_duplicates()

    print("")
    return df_friends, df_group

def getFriendsPrimary(primary):
    print("Gathering friends from " + ntpath.basename(primary))
    print("WARNING - WILL contain users that are not friends")
    conn = sqlite3.connect(primary)
    messagesQuery = """select
    snapchatter.userId,
    snapchatter.rowid,
    index_snapchatterusername.username
    from snapchatter
    inner join index_snapchatterusername ON snapchatter.rowid=index_snapchatterusername.rowid
    """
    df = pd.read_sql_query(messagesQuery, conn)

    df_group = pd.DataFrame(columns=['Group Name', 'Participants', 'Conversation ID'])
    
    print("")
    return df, df_group

def fixSenders(df_messages, df_friends):
    print("Replacing user ID with username in chats")
    try:
        array = []
        for index, row in df_friends.iterrows():
            lista = []
            lista.append(row["User ID"])
            lista.append(row["Username"])
            array.append(lista)
            
        for index, row in df_messages.iterrows():
            sender = row["sender_id"]
            for item in array:
                if sender == item[0]:
                    df_messages.loc[index, "sender_id"] = item[1]
    except:
        pass

    print("")
    return df_messages

def getCache(database):
    print("Getting cache files from "+ ntpath.basename(database))
    conn = sqlite3.connect(database)
    messagesQuery = f"""select
    *
    from CACHE_FILE_CLAIM where USER_ID is '{uuid}' and MEDIA_CONTEXT_TYPE is 3 and DELETED_TIMESTAMP_MILLIS is 0"""
    df = pd.read_sql_query(messagesQuery, conn)

    foundFiles = glob.glob(SCContentFolder+'*')
    tmp = []
    for item in foundFiles:
        item = item.replace("\\", "/")
        tmp.append(item)
    foundFiles = tmp
    
    for index, row in df.iterrows():
        try:
            file = SCContentFolder + row["CACHE_KEY"]
            if file in foundFiles:
                fileIndex = foundFiles.index(file)
                kind = filetype.guess(foundFiles[fileIndex])
                if os.stat(foundFiles[index]).st_size != 0 and kind != None:
                    if kind.extension == "mp4" or kind.extension == "jpg" or kind.extension == "png":
                        shutil.copy(foundFiles[fileIndex], outputDir + '//cacheFiles')
                    else:
                        df = df.drop(index) #("dropping because of invalid type")
                else:
                    df = df.drop(index) #("dropping because of 0 size")
            else:
                df = df.drop(index) #("dropping because of file not found")
        except Exception as E:
            print("Error copying cache files", E)

    print("") 
    return(df)

def getChats(database):
    print("Getting chats from "+ ntpath.basename(database))
    conn = sqlite3.connect(database)

    messagesQuery = """select
    client_conversation_id,
    server_message_id,
    message_content,
    datetime(creation_timestamp/1000, 'unixepoch') as 'Creation Timestamp',
    datetime(read_timestamp/1000, 'unixepoch') as 'Read Timestamp',
    content_type,
    sender_id
    from conversation_message
    order by client_conversation_id, creation_timestamp
    """

    df = pd.read_sql_query(messagesQuery, conn)

    for index, row in df.iterrows():
        message = (row["message_content"])
        messages = proto_to_msg(message)
        meddelande = ""
        try:
            if len(messages) >= 2:
                for i in messages:
                    tmp = i.encode('cp1252', 'xmlcharrefreplace')   #Display Emojis
                    tmp = tmp.decode('cp1252')
                    meddelande = meddelande + tmp
            else:
                i = messages[0].encode('cp1252', 'xmlcharrefreplace')
                meddelande = i.decode('cp1252')
                
            df.loc[index, "message_content"] = meddelande
        
        except Exception as e:
            df.loc[index, "message_content"] = """ERROR - Something went wrong when parsing this message. \n Manually verify the message with Client Conversation ID and Server Message ID in arroyo.db"""

    print("")
    return df

def mergeCacheChats(cache_df, chats_df, persistent_df):

    print("Merging chats with cache files")
    cache_df_v2 = pd.DataFrame(columns=['CACHE_KEY', 'TYPE', 'CONVERSATION_ID', 'SERVER_MESSAGE_ID'])
    for index, row in cache_df.iterrows():
        try:
            data = row["EXTERNAL_KEY"]
            data = data.split(":")
            try:
                tmp_dict = {'CACHE_KEY': row["CACHE_KEY"], 'TYPE': data[0], 'CONVERSATION_ID': data[1], 'SERVER_MESSAGE_ID': data[2], 'SERVER_MESSAGE_ID_PART': data[3]}
                
                cache_df_v2 = cache_df_v2.append(tmp_dict, ignore_index = True)
            except IndexError:
                pass
        except Exception as Error:
            print(Error)
    frames = [cache_df_v2, persistent_df]
    cache_df_v2 = pd.concat(frames, ignore_index = True)
            
    final_df = pd.DataFrame(columns=['Client Conversation ID', 'Message Content', 'Creation Timestamp', 'Read Timestamp', 'Content Type', 'Sender ID', 'Server Message ID'])
    for index_cache, row_cache in cache_df_v2.iterrows():
        match = False
        try:
            for index_chats, row_chats in chats_df.iterrows():
                if row_cache["CONVERSATION_ID"] == row_chats["client_conversation_id"] and int(row_cache["SERVER_MESSAGE_ID"]) == row_chats["server_message_id"]:
                    match = True
                    tmp_dict = {'Client Conversation ID': row_cache["CONVERSATION_ID"], 'Message Content': row_cache["CACHE_KEY"], 'Creation Timestamp': row_chats["Creation Timestamp"], 'Read Timestamp': row_chats["Read Timestamp"],
                                'Content Type': row_cache["TYPE"], 'Sender ID': row_chats["sender_id"], 'Server Message ID': str(row_cache["SERVER_MESSAGE_ID"]) + ":" + str(row_cache['SERVER_MESSAGE_ID_PART'])}
                    final_df = final_df.append(tmp_dict, ignore_index = True)
                else:
                    pass
        except Exception as Error:
            print(Error)
            
        if match == False:
            tmp_dict = {'Client Conversation ID': row_cache["CONVERSATION_ID"], 'Message Content': row_cache["CACHE_KEY"], 'Creation Timestamp': 'Unknown',
                                'Read Timestamp': 'Unknown', 'Content Type': row_cache["TYPE"], 'Sender ID': 'Unknown', 'Server Message ID': str(row_cache["SERVER_MESSAGE_ID"]) + ":" + str(row_cache['SERVER_MESSAGE_ID_PART'])}
            final_df = final_df.append(tmp_dict, ignore_index = True)
            
    for index, row_chats in chats_df.iterrows():
        if row_chats["content_type"] == 1: #Only adds text messages - other types are ignored for now
            tmp_dict = {'Client Conversation ID': row_chats["client_conversation_id"], 'Message Content': row_chats["message_content"], 'Creation Timestamp': row_chats["Creation Timestamp"],
                                'Read Timestamp': row_chats["Read Timestamp"], 'Content Type': row_chats["content_type"], 'Sender ID': row_chats["sender_id"], 'Server Message ID': row_chats["server_message_id"]}
            final_df = final_df.append(tmp_dict, ignore_index = True)

    print("")
    return final_df

def getSCPersistentMedia():

    persistent_df = pd.DataFrame(columns=['CACHE_KEY', 'TYPE', 'CONVERSATION_ID', 'SERVER_MESSAGE_ID'])
    path = snapchatFolder + "/Library/Caches/SCPersistentMedia/"
    files = os.listdir(Path(path))
    for file in files:
        file_path = path+file
        if os.stat(Path(file_path)).st_size != 0:
            outPutCache = Path(outputDir + '/cacheFiles/')
            shutil.copy(file_path, outPutCache)
        else:
            pass
        file_split = file.split("_")
        try:
            tmp_dict = {'CACHE_KEY': file, 'TYPE': file_split[0], 'CONVERSATION_ID': file_split[1], 'SERVER_MESSAGE_ID': file_split[2], 'SERVER_MESSAGE_ID_PART': file_split[3]}
            persistent_df = persistent_df.append(tmp_dict, ignore_index = True)
        except:
            pass

    return persistent_df


def main():
    global snapchatFolder
    global groupPlist
    global outputDir
    global SCContentFolder
    global uuid
    global platform
    
    if len(sys.argv) <2:
        print("ParseSnapChat.py <Snapchat folder> <Snapchat plist>")
        sys.exit()

    platform = system()
    print(platform)
    snapchatFolder = sys.argv[1]
    try:
        groupPlist = sys.argv[2]
    except:
        groupPlist = ""

    if os.path.exists(groupPlist) and os.path.exists(snapchatFolder):
        pass
    else:
        print("Group plist does not exist, cannot find Display Name")
        print("Using primary.docobjects instead")
        print("")
        groupPlist = ""
        
    if platform == "Windows":
        uuid32hex = re.compile('[0-9a-f]{32}')
        userPlist = Path(snapchatFolder + "/Documents/user.plist")
        uuid = getUserID(userPlist)
        arroyo = glob.glob("./"+snapchatFolder+"/Documents/user_scoped/**/*arroyo.db*", recursive = True)
        primaryDoc = glob.glob("./"+snapchatFolder+"/Documents/user_scoped/**/*primary.docobjects*", recursive = True)
        cacheController = glob.glob("./"+snapchatFolder+"/Documents/global_scoped/cachecontroller/*cache_controller.db*", recursive = True)
        html = ""
        outputDir = "./Snapchat_report_" + datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
        os.makedirs(outputDir+"//cacheFiles", exist_ok = True)
        SCContentFolder = snapchatFolder + "/Documents/com.snap.file_manager_3_SCContent_" + uuid + "/"
        
    elif platform == "Linux":
        uuid32hex = re.compile('[0-9a-f]{32}')
        userPlist = Path(snapchatFolder + "/Documents/user.plist")
        uuid = getUserID(userPlist)
        arroyo = glob.glob(snapchatFolder+"/Documents/user_scoped/**/*arroyo.db*", recursive = True)
        primaryDoc = glob.glob(snapchatFolder+"/Documents/user_scoped/**/*primary.docobjects*", recursive = True)
        cacheController = glob.glob(snapchatFolder+"/Documents/global_scoped/cachecontroller/*cache_controller.db*", recursive = True)
        html = ""
        outputDir = "./Snapchat_report_" + datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
        os.makedirs(outputDir+"//cacheFiles", exist_ok = True)
        SCContentFolder = snapchatFolder + "/Documents/com.snap.file_manager_3_SCContent_" + uuid + "/"

    else:
        print(f"Your platform {platform} might not be supported")
    
    if groupPlist != "":
        try:
            friends_df, group_df = getFriendsPlist(groupPlist)
        except Exception as Error:
            friends_df , group_df= getFriendsPrimary(Path(primaryDoc[0]))
    else:
        friends_df, group_df = getFriendsPrimary(Path(primaryDoc[0]))

    chats_df = getChats(arroyo[0])
    chats_df = fixSenders(chats_df, friends_df)
    cache_df = getCache(cacheController[0])
    persistent_df = getSCPersistentMedia()
    final_df = mergeCacheChats(cache_df, chats_df, persistent_df)
                    
    final_df = final_df.drop_duplicates()
    final_df = final_df.sort_values(by=['Client Conversation ID', 'Creation Timestamp'])

    print("Writing HTML report")
    for index, clientConversationID in final_df.groupby('Client Conversation ID'):
        html = html + clientConversationID.to_html(classes = 'table-striped', escape=False, col_space=100, justify='center', index=False, formatters={'Message Content':path_to_image_html})
      
    html = html + friends_df.to_html(classes = 'table-striped', escape=False, col_space=100, justify='center', index=False)
    html = html + group_df.to_html(classes = 'table-striped', escape=False, col_space=100, justify='center', index=False)

    text_file = open(outputDir + "/report.html", "w", encoding="cp1252")
    text_file.write(html)
    text_file.close()
    print("Success, report can be found in "+ os.path.abspath(outputDir))

if __name__ == "__main__":

    main()
    
    
