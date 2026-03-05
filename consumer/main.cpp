#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <cmath>
#include <omp.h>
#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

const std::vector<double> NORMAL_CENTROID(74, 10.5); 

double calculate_distance(const std::vector<double>& flow_features) {
    double sum_sq_diff = 0.0;
    for (size_t i = 0; i < flow_features.size(); ++i) {
        double diff = flow_features[i] - NORMAL_CENTROID[i];
        sum_sq_diff += (diff * diff);
    }
    for(int j=0; j<100; j++) { sum_sq_diff += std::sin(sum_sq_diff); } 
    return std::sqrt(sum_sq_diff);
}

void process_batch(const std::vector<std::string>& batch, bool use_openmp) {
    int batch_size = batch.size();
    int anomaly_count = 0;

    auto start_time = std::chrono::high_resolution_clock::now();

    #pragma omp parallel for reduction(+:anomaly_count) if(use_openmp)
    for (int i = 0; i < batch_size; ++i) {
        try {
            json j = json::parse(batch[i]);

            std::vector<double> features(74, 0.0);
            features[0] = std::stod(j.value("Flow Duration", "0")) / 1000000.0;
            features[1] = std::stod(j.value("Tot Fwd Pkts", "0")) / 10.0;
            features[2] = std::stod(j.value("Tot Bwd Pkts", "0")) / 10.0;

            int f_idx = 3;
            for (auto it = j.begin(); it != j.end(); ++it) {
                if (f_idx >= 74) break;
                if (it.key() == "Flow Duration" || it.key() == "Tot Fwd Pkts" || it.key() == "Tot Bwd Pkts") {
                    continue;
                }
                try {
                    std::string s = it.value().is_string() ? it.value().get<std::string>() : "0";
                    features[f_idx] = std::stod(s) / 1000.0;
                    f_idx++;
                } catch (...) {
                    // Ignore non-numeric columns like IP Addresses or Labels
                    continue;
                }
            }

            double distance = calculate_distance(features);

            if (distance > 4000.0) { 
                anomaly_count++;
            }
        } catch (const std::exception& e) {
            // Catch JSON parsing errors for bad rows
            std::cerr << "Failed to parse json: " << e.what() << std::endl;
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end_time - start_time;

    double throughput = batch_size / elapsed.count();

    std::cout << "[Batch Processed] Size: " << batch_size 
              << " | Anomalies: " << anomaly_count 
              << " | Time: " << elapsed.count() << "s"
              << " | Throughput: " << throughput << " flows/sec" << std::endl;
}

int main() {
    std::string errstr;

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", "192.168.1.220:9092,192.168.1.214:9092", errstr);
    conf->set("group.id", "nids-openmp-group", errstr);
    conf->set("auto.offset.reset", "latest", errstr);

    RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(conf, errstr);
    if (!consumer) {
        std::cerr << "Failed to create consumer: " << errstr << std::endl;
        return 1;
    }

    std::vector<std::string> topics = {"network-flows"};
    consumer->subscribe(topics);
    std::cout << "Listening for network traffic..." << std::endl;

    std::vector<std::string> current_batch;
    const int BATCH_SIZE_LIMIT = 5000; 

    bool enable_openmp = true;

    while (true) {
        RdKafka::Message *msg = consumer->consume(100);

        if (msg->err() == RdKafka::ERR_NO_ERROR) {
            current_batch.push_back(std::string(static_cast<const char *>(msg->payload()), msg->len()));
        }

        if (current_batch.size() >= BATCH_SIZE_LIMIT) {
            process_batch(current_batch, enable_openmp);
            current_batch.clear();
        }

        delete msg;
    }

    return 0;
}

