from confluent_kafka import Producer

# Read AMI data
path = "/home/vietan/kafka/datasets/ami/azElaaanzza.txt"
# path = "/home/vietan/kafka/datasets/ami/hv_lp_1/waFduwwwf7dn.txt"

data_src = []

with open(path, 'r') as file:
    data = file.readlines()
    for i, line in enumerate(data):
        if i < 4: 
            continue
        line_t = line.replace('\n', '').split(' | ')
        data_src.append(line_t[-1])

print(len(data_src))
#print(max(data_src)) 2.068 
#print(min(data_src)) 0.0

def delivery_report(err, msg):
    if err is not None:
        print("Failed to deliver message: {0}: {1}"
              .format(msg.value(), err.str()))
    else:
        # print("Message produced: {0}".format(msg.value()))
        print('Massage delivered to {} [{}]'.format(msg.topic(), msg.value()))

#p = Producer({'bootstrap.servers': 'localhost:9092'})
p = Producer({'bootstrap.servers': '143.248.152.217:9092'})

try:
    for i, data in enumerate(data_src):
        
        # from beginning
        # p.produce('AMI-ID-azElaaanzza', data, callback=delivery_report)
        # p.poll(0.1)

#        if i >= 16128: # from month #6        
#         if i >= 2688: # from month #1
        
       if i >= 8064: # from month #3
            p.produce('AMI-ID-azElaaanzza', data, callback=delivery_report)
            p.poll(0.1)
                   
#        if i >= 8064: # from month #3
#             p.produce('AMI-ID-waFduwwwf7dn-hv_lp_1', data, callback=delivery_report)
#             p.poll(0.1)       
       
#        if i >= 27264+128: # batch 128; 150 values
#             break


#        if i > 40832+256: # test #epoch_effects, batch size = 128, take 300 vals 
#             break

#        if i >= 46464: # test #epoch_effects, batch size = 128, take 300 vals 
#             break


#        if i >= 50000: # from month #3
#             p.produce('AMI-ID-azElaaanzza', data, callback=delivery_report)
#             p.poll(0.1)
       
#        if i > 82768+256: # test #epoch_effects, batch size = 128, take 300 vals 
#             break
            
except KeyboardInterrupt:
    pass

p.flush(30)
