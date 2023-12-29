import os
import sys
from server_utils import *

class MyMiniDFS:
    def __init__(self):
        self.gconf = GlobalConfig()

        # Establish myminidfs directory
        self.setup_myminidfs_dir()

        # Launch name_servers and data_servers
        self.launch_servers()

    def setup_myminidfs_dir(self):
        if not os.path.isdir("myminidfs"):
            os.makedirs("myminidfs")
            for i in range(NUM_DATA_SERVER):
                os.makedirs(f"myminidfs/DataServer{i}")
            os.makedirs("myminidfs/NameServer")

    def launch_servers(self):
        name_server = NameNode('NameServer', self.gconf)
        name_server.start()

        data_servers = [DataNode(s_id, self.gconf) for s_id in range(NUM_DATA_SERVER)]
        for server in data_servers:
            server.start()

    def parse_cmd(self, cmd):
        cmds = cmd.split()
        flag = False

        if len(cmds) >= 1 and cmds[0] in operation_names:
            flag = self.handle_operation(cmds[0], cmds[1:])
        else:
            print('Usage: upload|read|download|quit|ls')

        return flag

    def handle_operation(self, operation, args):
        flag = False

        if operation == operation_names[0]:
            flag = self.handle_upload(args)
        elif operation == operation_names[1]:
            flag = self.handle_read(args)
        elif operation == operation_names[2]:
            flag = self.handle_download(args)
        elif operation == operation_names[3]:
            flag = self.handle_quit(args)
        elif operation == operation_names[4]:
            flag = self.handle_ls(args)

        return flag

    def handle_upload(self, args):
        flag = False

        if len(args) == 1:
            source_file_path = args[0]
            if os.path.isfile(source_file_path):
                self.gconf.file_path = source_file_path
                self.gconf.cmd_type = COMMAND.upload
                flag = True
            else:
                print('Warning: please check the input file exists...')
        else:
            print('Warning: <upload source_file_path>')

        return flag

    def handle_read(self, args):
        flag = False

        if len(args) == 3:
            try:
                self.gconf.file_id = int(args[0])
                self.gconf.read_offset = int(args[1])
                self.gconf.read_length = int(args[2])
                self.gconf.cmd_type = COMMAND.read
                flag = True
            except ValueError:
                print('Warning: please check file_id, offset, length are integers...')
        else:
            print('Warning: <read file_id offset length>')

        return flag

    def handle_download(self, args):
        flag = False

        if len(args) == 2:
            file_id, save_path = args
            try:
                self.gconf.file_id = int(file_id)
                self.gconf.download_savepath = save_path
                base = os.path.split(save_path)[0]
                if len(base) > 0 and not os.path.exists(base):
                    print('Warning: please check save_path exists...')
                else:
                    self.gconf.cmd_type = COMMAND.download
                    flag = True
            except ValueError:
                print('Warning: file_id should be integer')
        else:
            print('Warning: <download file_id save_path>')

        return flag

    def handle_quit(self, args):
        flag = False

        if not args:
            print_info('Shut down')
            print("Exiting...")
            flag = True
            self.gconf.cmd_type = COMMAND.quit
            os._exit(0)
        else:
            print('Warning: quit')

        return flag

    def handle_ls(self, args):
        flag = False

        if not args:
            flag = True
            self.gconf.cmd_type = COMMAND.ls
        else:
            print('Warning: ls')

        return flag

    def run(self):
        cmd_prompt = 'MyMiniDFS > '
        print(cmd_prompt, end='')

        while True:
            cmd_str = input()
            self.gconf.cmd_flag = self.parse_cmd(cmd_str)

            if self.gconf.cmd_flag:
                if self.gconf.cmd_type == COMMAND.quit:
                    sys.exit(0)

                # transfer the signal to name_server
                self.gconf.name_event.set()

                # Handle specific commands
                self.handle_specific_commands()

            print(cmd_prompt, end='')

    def handle_specific_commands(self):
        if self.gconf.cmd_type == COMMAND.upload:
            self.handle_upload_command()
        elif self.gconf.cmd_type == COMMAND.read:
            self.handle_read_command()
        elif self.gconf.cmd_type == COMMAND.ls:
            self.handle_ls_command()
        elif self.gconf.cmd_type == COMMAND.download:
            self.handle_download_command()

    def handle_upload_command(self):
        for i in range(NUM_DATA_SERVER):
            self.gconf.main_events[i].wait()
        print('Upload Succeed: File ID %d' % (self.gconf.file_id,))
        self.gconf.server2chunk.clear()
        for i in range(NUM_DATA_SERVER):
            self.gconf.main_events[i].clear()

    def handle_read_command(self):
        self.gconf.read_event.wait()
        self.gconf.read_event.clear()

    def handle_ls_command(self):
        self.gconf.ls_event.wait()
        self.gconf.ls_event.clear()

    def handle_download_command(self):
        for i in range(NUM_DATA_SERVER):
            self.gconf.main_events[i].wait()

        if self.gconf.download_chunks > 0:
            self.download_file()
            print('Download Succeed!')

        for i in range(NUM_DATA_SERVER):
            self.gconf.main_events[i].clear()

    def download_file(self):
        f_download = open(self.gconf.download_savepath, mode='wb')
        for i in range(self.gconf.download_chunks):
            server_id = self.gconf.download_servers[i]
            chunk_file_path = f"myminidfs/DataServer{server_id}/{self.gconf.file_id}-chunk-{i}"
            chunk_file = open(chunk_file_path, "rb")
            f_download.write(chunk_file.read())
            chunk_file.close()
        f_download.close()

def print_info(op):
    print(op, 'MyMiniDFS: ')
    pstr = 'NameServer + ' + " + ".join([f'DataServer{i}' for i in range(NUM_DATA_SERVER)])
    print(pstr)

if __name__ == '__main__':
    print_info('Launch')
    my_mini_dfs = MyMiniDFS()
    my_mini_dfs.run()
