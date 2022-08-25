# ParseSnapchat | OLD
# Use https://github.com/DFIR-HBG/Snapchat_Auto
iOS Snapchat parser for chats and cached files\
Tested on Windows and Linux

\
install required libraries:\
pip install -r requirements.txt\
ccl_bplist and parse3 are included here since they are not avaliable from pip
 
  \
Usage:
  
Extract /private/var/mobile/Containers/Data/Application/\<Snapchat-GUID-Folder>/
and /private/var/mobile/Containers/Shared/AppGroup/\<Snapchat-GUID-Folder>/group.snapchat.picaboo.plist
  
Place the folder, file and script in a new folder

Run: ParseSnapchat.exe \<Snapchat folder> \<Snapchat plist> 
 
