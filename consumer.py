"""An implementation of Data preprocesor, model retraining on Kafka consumer.
"""
from confluent_kafka import Consumer, KafkaError

# LSTM for international airline passengers problem with regression framing
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import sys
import time
import json
import numpy
import math
import matplotlib.pyplot as plt
import numpy as np
from math import sqrt
from keras.layers import Input, LSTM, Dense, Embedding, Activation, Dropout, Flatten
from keras.layers.convolutional import Conv1D
from keras.layers.convolutional import MaxPooling1D

from keras.models import Sequential, load_model
from keras.optimizers import SGD

from sklearn.metrics import mean_squared_error
from keras.callbacks import ModelCheckpoint, EarlyStopping

LEARNING_RATE = 0.0001
EPOCHS = 50

# max/min values of AMI data for normalization
MAX_VAL = 2.068  
MIN_VAL = 0.0

# TIME_STEPS = 36
# BATCH_SIZE_TRAIN = 128
# BATCH_RECV_SIZE = 128
# BUFFER_ALL_DATA_SIZE = 16128 # (6 months) no. batches (weeks) to store from begining (might include old and new data)

TIME_STEPS = 36
BATCH_SIZE_TRAIN = 124
BATCH_RECV_SIZE = 124
BUFFER_ALL_DATA_SIZE = 16128 # (6 months) no. batches (weeks) to store from begining (might include old and new data)

# configure Kafka Consumer
c = Consumer({
    'bootstrap.servers': '143.248.152.217:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest',
    'max.poll.interval.ms': 86400000
})

c.subscribe(['AMI-ID-azElaaanzza'])
# c.subscribe(['AMI-ID-azElaaanzEE'])

_batch_recv = []
_x_data = []
_y_data = []
_x_data_nWeeks = []
_y_data_nWeeks = []
_cnt_batch = 0
_x_buffer_all_data = np.array([])
_y_buffer_all_data = np.array([])

def preprocess(data):
    """
    create time steps data to feed the model. 
    """
    global _batch_recv, _x_data, _y_data
    available = False
 
    _batch_recv.append((data-MIN_VAL)/(MAX_VAL-MIN_VAL)) # normalize data
    
    if len(_batch_recv) > TIME_STEPS + 1:
        _batch_recv.pop(0)
        _x_data.append(_batch_recv[0:-1])
        _y_data.append(_batch_recv[-1])

        if len(_y_data) == BATCH_RECV_SIZE:
            _x = np.array(_x_data)
            _y = np.array(_y_data)
            available = True
            _x_data = []
            _y_data = []
            return available, _x.reshape(_x.shape[0], _x.shape[1], 1), _y
    return available, '', ''

def compute_rmse(model, x, y):
    y_pred = model.predict(x)
    y_pred_org = y_pred*(MAX_VAL-MIN_VAL) + MIN_VAL
    y_train_org = y*(MAX_VAL-MIN_VAL) + MIN_VAL
    y_train_org = y_train_org.reshape(y_train_org.shape[0], 1)
    # L1 Error            
    L1_error_batch = abs(y_pred_org - y_train_org)          
    # RMSE
    rmse = sqrt(mean_squared_error(y_pred_org, y_train_org))
    return rmse

def preprocess_batch(data, batch_size):
    """
    create time steps data to feed the model. 
    """
    global _batch_recv, _x_data, _y_data
    available = False
 
    _batch_recv.append((data-MIN_VAL)/(MAX_VAL-MIN_VAL)) # normalize data
    
    if len(_batch_recv) > TIME_STEPS + 1:
        _batch_recv.pop(0)
        _x_data.append(_batch_recv[0:-1])
        _y_data.append(_batch_recv[-1])

        if len(_y_data) == batch_size:
            _x = np.array(_x_data)
            _y = np.array(_y_data)
            available = True
            _x_data = []
            _y_data = []
            return available, _x.reshape(_x.shape[0], _x.shape[1], 1), _y
    return available, '', ''


model_infer = load_model("/home/vietan/kafka/pre-trained-models/soyoon/sy_best_model_3month_azElaaanzza.h5")

# model_update = load_model("/home/vietan/kafka/pre-trained-models/soyoon/sy_best_model_3month_azElaaanzza.h5")

cnt_train_batch = 0
cnt_inst = 0
is_first_batch = True
sum_train_time = 0
rmse_arr = []

# parameters for adaptive batch size
MAX_DATA_SIZE = 196
MIN_DATA_SIZE = 64
data_size_arr = range(MIN_DATA_SIZE, MAX_DATA_SIZE)
mean_err = 0.5
alpha = 0.15
WINDOW_SIZE = 5
rmse_window = []
MAX_RMSE = 0.23160652830908154
MIN_RMSE = 0.10356675641214313

cnt_timeliness = 0
timeliness_arr = []

def compute_timeliness(data_size_arr):
    sum_t = 0
    for d in data_size_arr:
        sum_t += d*(d-1)/2
    return sum_t

def adjust_batch_func(x, err):
    return alpha*x*(x-1)/float(MAX_DATA_SIZE*(MAX_DATA_SIZE-1)) + (1-alpha)*math.exp(-x/MAX_DATA_SIZE)*(err) 

# batch_size = 128
batch_size = 155

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            cnt_inst += 1
            # print('Received message: {0}'.format(msg.value()))
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            print('Error occured: {0}'.format(msg.error().str()))

        # available, x_train, y_train = preprocess(float(msg.value()))

        available, x_train, y_train = preprocess_batch(float(msg.value()), batch_size)
        
        if available:   
            x_train = x_train.reshape(x_train.shape[0], 6, 6)
            
            y_pred = model_infer.predict(x_train)
            y_pred_org = y_pred*(MAX_VAL-MIN_VAL) + MIN_VAL
            y_train_org = y_train*(MAX_VAL-MIN_VAL) + MIN_VAL
            y_train_org = y_train_org.reshape(y_train_org.shape[0], 1)
            
            # L1 Error            
            L1_error_batch = abs(y_pred_org - y_train_org)          

            # RMSE
            rmse = sqrt(mean_squared_error(y_pred_org, y_train_org))
            print(rmse)
            rmse_arr.append(rmse)
                            
# ---------------------------- Update ----------------------------------
            cnt_train_batch += 1
    
            start = time.time()
            model_infer.fit(x_train, y_train, epochs=25, verbose=0, batch_size=batch_size, shuffle=False) 
            end = time.time()
            sum_train_time += end - start
            
            optimal_D = 128
            min_val = obj_func(m, alpha, beta, E, delta, 128, rmse)
            
            for x in range(129,256):
                val = obj_func(m, alpha, beta, E, delta, x, rmse)
                if val < min_val:
                    optimal_D = x
            print("optimal_D: ", x)
            
            batch_size = optimal_D
            
            if cnt_train_batch >= 32:
                print("Training time: ", sum_train_time)
                print("#batch: {0} | mean RMSE: {1}".format(cnt_train_batch, np.mean(rmse_arr)))

                
except KeyboardInterrupt:
    pass

finally:
    c.close()