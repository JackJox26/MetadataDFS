import os
import pickle
import math
import threading
from random import choice
from enum import Enum

# DFS Meta Data
CHUNK_SIZE = 2 * 1024 * 1024
NUM_DATA_SERVER = 4
NUM_REPLICATION = 3
CHUNK_PATTERN = '%s-chunk-%s'

# Name Info Path
NAME_INFO_PATH = './myminidfs/NameServer/info.pkl'

# Data Node
DATA_NODE_DIR = './myminidfs/DataServer%s'

LS_PATTERN = '%s\t%20s\t%10s'

# Operations
operation_names = ('upload', 'read', 'download', 'quit', 'ls')
COMMAND = Enum('COMMAND', operation_names)

class GlobalConfig:
    def __init__(self):
        # global variables
        self.server2chunk = {}  # datanode -> chunks
        self.read_chunk = None
        self.read_offset = None
        self.read_length = None

        self.cmd_flag = False
        self.file_id = None
        self.file_dir = None  # read or fetch using dir/filename
        self.file_path = None  # local source path
        self.cmd_type = None

        self.download_savepath = None  # download on local
        self.download_servers = []
        self.download_chunks = None

        # events
        self.name_event = threading.Event()
        self.ls_event = threading.Event()
        self.read_event = threading.Event()

        self.data_events = [threading.Event() for _ in range(NUM_DATA_SERVER)]
        self.main_events = [threading.Event() for _ in range(NUM_DATA_SERVER)]



class NameNode(threading.Thread):
    def __init__(self, name, gconf):
        super(NameNode, self).__init__(name=name)
        self.gconf = gconf
        self.infos = None
        self.fid2chunk_map = None        #{file_id: [fileid-chunk-0,fileid-chunk-1, ...]}
        self.id2file_map = None         #{file_id: (file_name, file_length)}
        self.chunk2server_map = None    #{chunk0: [server_m, server_n,...]}
        self.pre_file_id = -1
        self.pre_data_server_id = -1
        self.load_info()

    def run(self):
        gconf = self.gconf
        while True:
            gconf.name_event.wait()

            if gconf.cmd_flag:
                if gconf.cmd_type == COMMAND.upload:
                    self.upload()
                elif gconf.cmd_type == COMMAND.read:
                    self.read()
                elif gconf.cmd_type == COMMAND.download:
                    self.download()
                elif gconf.cmd_type == COMMAND.ls:
                    self.ls()
                else:
                    pass
            gconf.name_event.clear()

    def load_info(self):
        ## load previous/current info

        if not os.path.isfile(NAME_INFO_PATH):
            self.infos = {
                'fid2chunk_map': {},
                'id2file_map': {},
                'chunk2server_map': {},
                'pre_file_id': -1,
                'pre_data_server_id': -1,
            }
        else:
            with open(NAME_INFO_PATH, 'rb') as f:
                self.infos = pickle.load(f)
        self.fid2chunk_map = self.infos['fid2chunk_map']
        self.id2file_map = self.infos['id2file_map']
        self.chunk2server_map = self.infos['chunk2server_map']
        self.pre_file_id = self.infos['pre_file_id']
        self.pre_data_server_id = self.infos['pre_data_server_id']

    def update_info(self):
        # update Name Info Data after upload

        with open(NAME_INFO_PATH, 'wb') as f:
            self.infos['pre_file_id'] = self.pre_file_id
            self.infos['pre_data_server_id'] = self.pre_data_server_id
            pickle.dump(self.infos, f)

    def ls(self):
        print('Total number of files: ', len(self.id2file_map))
        print(LS_PATTERN % ('File_ID', 'File_Name', 'File_Length'))
        for file_id, (file_name, file_len) in self.id2file_map.items():
            print(LS_PATTERN % (file_id, file_name, file_len))
        self.gconf.ls_event.set()

    def upload(self):
        ## split -> distribute
        source_file_path = self.gconf.file_path
        file_name = source_file_path.split('/')[-1]
        self.pre_file_id += 1
        server_id = (self.pre_data_server_id + 1) % NUM_REPLICATION
        file_length = os.path.getsize(source_file_path)
        chunks = int(math.ceil(float(file_length) / CHUNK_SIZE))

        # update fid2chunk_map & id2file_map
        self.fid2chunk_map[self.pre_file_id] = [CHUNK_PATTERN % (self.pre_file_id, i) for i in range(chunks)]
        self.id2file_map[self.pre_file_id] = (file_name, file_length)

        for i, chunk in enumerate(self.fid2chunk_map[self.pre_file_id]):
            self.chunk2server_map[chunk] = []
            # assign each chunk to data servers
            for j in range(NUM_REPLICATION):
                assign_server = (server_id + j) % NUM_DATA_SERVER
                self.chunk2server_map[chunk].append(assign_server)
                # update chunk-server info: server2chunk
                size_in_chunk = CHUNK_SIZE if i < chunks - 1 else file_length % CHUNK_SIZE
                if assign_server not in self.gconf.server2chunk:
                    self.gconf.server2chunk[assign_server] = []
                self.gconf.server2chunk[assign_server].append((chunk, CHUNK_SIZE * i, size_in_chunk))

            server_id = (server_id + NUM_REPLICATION) % NUM_DATA_SERVER

        self.pre_data_server_id = (server_id - 1) % NUM_DATA_SERVER
        self.update_info()

        self.gconf.file_id = self.pre_file_id
        for data_event in self.gconf.data_events:
            data_event.set()

    def read(self):
        ## transfer read signal to each data node
        gconf = self.gconf
        file_id = gconf.file_id
        read_offset = gconf.read_offset
        read_length = gconf.read_length

        if file_id not in self.id2file_map:
            print('Cannot find a file with id', file_id)
            gconf.read_event.set()
        elif read_offset < 0 or read_length < 0:
            print('Offset or length cannot be less than 0')
            gconf.read_event.set()
        elif (read_offset + read_length) > self.id2file_map[file_id][1]:
            print('Check if the offset and length meet the requirements...', self.id2file_map[file_id][1])
            gconf.read_event.set()
        else:
            start_chunk = int(math.floor(read_offset / CHUNK_SIZE))
            space_left_in_chunk = (start_chunk + 1) * CHUNK_SIZE - read_offset

            if space_left_in_chunk < read_length:
                print('Cannot read across chunks')
                gconf.read_event.set()
            else:
                read_server_candidates = self.chunk2server_map[CHUNK_PATTERN % (file_id, start_chunk)]
                read_server_id = choice(read_server_candidates)
                gconf.read_chunk = CHUNK_PATTERN % (file_id, start_chunk)
                gconf.read_offset = read_offset - start_chunk * CHUNK_SIZE
                gconf.data_events[read_server_id].set()


    def download(self):
        gconf = self.gconf
        file_id = gconf.file_id

        if file_id not in self.id2file_map:
            gconf.download_chunks = -1
            print('No such file with id =', file_id)
        else:
            file_chunks = self.fid2chunk_map[file_id]
            gconf.download_chunks = len(file_chunks)
            for chunk in file_chunks:
                gconf.download_servers.append(self.chunk2server_map[chunk][0])
            for data_event in gconf.data_events:
                data_event.set()
 



class DataNode(threading.Thread):
    def __init__(self, server_id, gconf):
        super(DataNode, self).__init__(name='DataServer%s' % (server_id,))
        self.gconf = gconf
        self._server_id = server_id

    def run(self):
        gconf = self.gconf
        while True:
            gconf.data_events[self._server_id].wait()
            if gconf.cmd_flag:
                if gconf.cmd_type == COMMAND.upload and self._server_id in gconf.server2chunk:
                    self.save()
                elif gconf.cmd_type == COMMAND.read:
                    self.read()
                else:
                    pass
            gconf.data_events[self._server_id].clear()
            gconf.main_events[self._server_id].set()

    def save(self):
        data_node_dir = DATA_NODE_DIR % (self._server_id,)
        with open(self.gconf.file_path, 'r') as f_in:
            for chunk, offset, count in self.gconf.server2chunk[self._server_id]:
                f_in.seek(offset, 0)
                content = f_in.read(count)
                with open(data_node_dir + os.path.sep + chunk, 'w') as f_out:
                    f_out.write(content)
                    f_out.flush()

    def read(self):
        read_path = (DATA_NODE_DIR % (self._server_id,)) + os.path.sep + self.gconf.read_chunk
        with open(read_path, 'r') as f_in:
            f_in.seek(self.gconf.read_offset)
            content = f_in.read(self.gconf.read_length)
            print(content)
        self.gconf.read_event.set()