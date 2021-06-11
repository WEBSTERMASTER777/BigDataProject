import pymongo
import  yaml


class MongoConnector:
    def __init__(self, yml='mongo_config.yaml', collection=None):

        try:
            with open(yml, "r") as exec_ini:
                cfg = yaml.load(exec_ini)
        except Exception as e:
            print("[-] Database connection error!")
            print(e)
        else:
            db_host = cfg['host']
            db_username = cfg['username']
            db_password = cfg['password']
            db_name = cfg['db_name']
            collection_name = cfg['collection_name']

            if collection:
                collection_name = collection

            try:
                client = pymongo.MongoClient(f"mongodb://{db_username}:{db_password}@{db_host}")
                db = client[db_name]
                self.table = db[collection_name]
                self.bulk = self.table.initialize_ordered_bulk_op()
                self.bulk_counter = 0

                print("[+] Database connected!")
            except Exception as e:
                print("[-] Database connection error!")
                print('1')
                print(e)

    def read_repeated_detections(self, n=3, timeframe=[]):
        key = f'repeated_detection.{n}'
        pipeline = {key: {'$exists': True}}
        if timeframe:
            pipeline['time_detected'] = {'$gt': timeframe[0],
                                         '$lt': timeframe[1]}
        try:
            result = self.table.find(pipeline, {'_id': 0, 'y': 1, 'x': 1, 'time_detected': 1, 'conf': 1,
                                                'repeated_detection': 1})
            result = [res for res in result]
        except Exception as e:
            print('[-] Unable to read repeated detections')
            print(e)
        else:
            return result if result else []

    def read_active(self, time_lag, *additional_filters):
        pipeline = [
            {'$sort': {'_id': -1}},
            {'$limit': 25},
            {'$match': {'time_detected': {'$gt': time_lag}}},
        ]
        for f in additional_filters:
            pipeline[2]['$match'][f[0]] = f[1]
        try:
            result = self.table.aggregate(pipeline)
            result = [res for res in result]

        except Exception as e:
            print("[-] Unable to read bd")
            print(e)

        else:
            return result

    def add_bulk(self, trash_id, field_names, field_values):
        try:
            self.bulk.find({'_id': trash_id}).update({
                '$set': {name: value for name, value in zip(field_names, field_values)}
            })
            self.bulk_counter += 1
        except Exception as e:
            print(f'[-] Unable to add bulk fields: {field_names}')
            print(e)

    def execute_bulk_update(self):
        if self.bulk_counter != 0:
            try:
                self.bulk.execute()
                self.bulk = self.table.initialize_ordered_bulk_op()
                self.bulk_counter = 0
            except Exception as e:
                print('[-] Unable to execute bulk update')
                print(e)

    def add_detection(self,
                      time_detected,
                      object_id,
                      category,
                      conf):

        item = {
            # "x": x,
            # "y": y,
            # "z": z,
            "time_detected": time_detected,
            "object_id": object_id,
            # "camera_id": camera_id,
            "category": category,
            # "clf_category": clf_category,
            "conf": conf,
            # "picked": False,
            # "proceed": {
            #     "proceed_1": "",
            #     "proceed_2": ""
            # },
            # "repeated_detection": []
        }

        try:
            item_id = self.table.insert_one(item).inserted_id
        except Exception as e:
            print("[-] Unable to write new detection")
            print(e)
        else:
            print(f"[+] Item {item_id} added.")
            return item_id

    def add_repeated_detection(self,
                               matched_obj,
                               time_detected,
                               x, y, z,
                               category,
                               clf_category,
                               conf):

        item = {
            "time_detected": time_detected,
            "x": x,
            "y": y,
            "z": z,
            "category": category,
            "clf_category": clf_category,
            "conf": conf
        }
        try:
            self.table.update_one({'_id': matched_obj['_id']}, {'$push': {'repeated_detection': item}})
        except Exception as e:
            print("[-] Unable to write repeated detection")
            print(e)
        else:
            print(f"[+] Item {matched_obj['_id']} updated.")

    def add_vconv(self, speed, timestamp, time_window, sigma, threshold):
        try:
            item_id = self.table.insert_one({'speed': speed,
                                             'time': timestamp,
                                             'time_window': time_window,
                                             'sigma': sigma,
                                             'threshold': threshold}).inserted_id
        except Exception as e:
            print("[-] Unable to write vconv")
            print(e)
        else:
            print(f"[+] Item {item_id} added.")
            return item_id

    def read_vconv(self):
        pipeline = [
            {'$sort': {'_id': -1}},
            {'$limit': 1},
        ]
        try:
            result = self.table.aggregate(pipeline)
            result = [res for res in result]
        except Exception as e:
            print("[-] Unable to read bd")
            print(e)
        else:
            return result[0]

    def add_height(self, array, timestamp):
        try:
            item_id = self.table.insert_one({'time': timestamp,
                                             'height': array}).inserted_id
        except Exception as e:
            print("[-] Unable to write height array")
            print(e)
        else:
            print(f"[+] Height array {item_id} added.")
            return item_id

    def read_height(self, time_from, time_to, timestamp):
        pipeline = [
            {'$match': {'time':
                            {'$gt': time_from,
                             '$lt': time_to}}
            }
        ]
        try:
            result = self.table.aggregate(pipeline)
            result = min([res for res in result], key=lambda x: abs(x['time']-timestamp))
        except ValueError as e:
            print("[-] No heights in time range")
        except Exception as e:
            print("[-] Unable to read bd")
            print(e)
        else:
            return result['height']
