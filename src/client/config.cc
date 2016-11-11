#include "config.h"

#include <iostream>
#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/writer.h>

namespace baidu {
namespace shuttle {

int Configuration::ParseCommandLine(int argc, char** argv) {
    namespace po = boost::program_options;
    std::vector<std::string> jobconf;
    po::options_description options("Options");
    options.add_options()
        ("help,h", "")
        ("all,a", "")
        ("immediate,i", "")
        ("input", po::value< std::vector<std::string> >(), "")
        ("output", po::value<std::string>(), "")
        ("file,f", po::value< std::vector<std::string> >(), "")
        ("cacheArchive", po::value<std::string>(), "")
        ("mapper", po::value<std::string>(), "")
        ("reducer", po::value<std::string>(), "")
        ("combiner", po::value<std::string>(), "")
        ("partitioner", po::value<std::string>(), "")
        ("inputformat", po::value<std::string>(), "")
        ("outputformat", po::value<std::string>(), "")
        ("jobconf", po::value< std::vector<std::string> >(&jobconf), "")
        ("nexus", po::value<std::string>(), "")
        ("nexus-file", po::value<std::string>(), "")
        ("nexus-root", po::value<std::string>(), "")
        ("master", po::value<std::string>(), "")
        ("command", po::value<std::string>()->required(), "")
        ("subcommand", po::value<std::string>(), "");
    po::positional_options_description positional;
    positional.add("command", 1);
    positional.add("subcommand", 1);
    boost::program_options::variables_map vars;
    try {
        po::command_line_parser parser(argc, argv);
        po::store(parser.options(options).positional(positional).run(), vars);
        po::notify(vars);
    } catch (const po::error& ex) {
        std::cerr << "ERROR: " << ex.what() << std::endl;
        return -1;
    }
    for (std::vector<std::string>::iterator it = jobconf.begin();
            it != jobconf.end(); ++it) {
        size_t colon = it->find_first_of("=");
        if (colon == std::string::npos) {
            continue;
        }
        kv_[it->substr(0, colon)] = it->substr(colon + 1);
    }
    for (po::variables_map::iterator it = vars.begin();
            it != vars.end(); ++it) {
        if (it->first == "input" || it->first == "files") {
            multivalue_[it->first] = it->second.as< std::vector<std::string> >();
        } else if (it->first == "jobconf") {
            continue;
        } else {
            kv_[it->first] = it->second.as<std::string>();
        }
    }
    return 0;
}

int Configuration::ParseJson(std::istream& is) {
    rapidjson::IStreamWrapper isw(is);
    rapidjson::Document doc;
    doc.ParseStream(isw);
    if (!doc.IsObject()) {
        std::cerr << "ERROR: json root should be an object" << std::endl;
        return -1;
    }
    if (!doc.HasMember("name") || !doc["name"].IsString()) {
        std::cerr << "ERROR: please provide a string as job name" << std::endl;
        return -1;
    }
    kv_["mapred.job.name"] = std::string(doc["name"].GetString(),
            doc["name"].GetStringLength());
    std::vector<std::string>& file_vec = multivalue_["files"];
    if (doc.HasMember("files")) {
        rapidjson::Value& files = doc["files"];
        if (files.IsArray()) {
            for (rapidjson::Value::ConstValueIterator it = files.Begin();
                    it != files.End(); ++it) {
                if (it->IsString()) {
                    file_vec.push_back(std::string(it->GetString(), it->GetStringLength()));
                } else {
                    std::cerr << "ERROR: files contains non-string member" << std::endl;
                    return -1;
                }
            }
        } else {
            std::cerr << "ERROR: files must be an array" << std::endl;
            return -1;
        }
    }
    if (doc.HasMember("cache_archive")) {
        if (doc["cache_archive"].IsString()) {
            kv_["cacheArchive"] = std::string(doc["cache_archive"].GetString(),
                    doc["cache_archive"].GetStringLength());
        } else {
            std::cerr << "ERROR: please provide a string as job name" << std::endl;
            return -1;
        }
    }
    if (!doc.HasMember("pipe") || !doc["pipe"].IsString()) {
        std::cerr << "ERROR: please provide a string as pipe" << std::endl;
        return -1;
    }
    // TODO check subcommand
    kv_["subcommand"] = std::string(doc["pipe"].GetString(), doc["pipe"].GetStringLength());
    if (doc.HasMember("split_size")) {
        if (doc["split_size"].IsNumber() && !doc["split_size"].IsDouble()) {
            kv_["mapred.input.split.size"] =
                boost::lexical_cast<std::string>(doc["split_size"].GetInt64());
        } else {
            std::cerr << "ERROR: please provide an integer as split size" << std::endl;
            return -1;
        }
    }
    return 0;
}

/*int Configuration::BuildJobDescription(sdk::JobDescription& job) const {
    if (vars_.count("count")) {
        const std::string& command = vars_["count"].as<std::string>();
        if (command == "streaming") {
            job.set_pipe_style(kStreaming);
        } else if (command == "bistreaming") {
            job.set_pipe_style(kBiStreaming);
        } else {
            std::cerr << "INTERNAL ERROR: command " << command
                      << " is not for submitting job" << std::endl;
            return -1;
        }
    } else {
        std::cerr << "INTERNAL ERROR: no command to submit" << std::endl;
        return -1;
    }
    std::map<std::string, std::string>::iterator it;
    if ((it = conf_.find("mapred.job.name")) == conf_.end()) {
        std::cerr << "ERROR: please offer the name of job" << std::endl;
        return -1;
    }
    job.name = it->second;
    if (vars_.count("file")) {
        const std::vector<std::string>& files =
            vars_["file"].as< std::vector<std::string> >();
        for (std::vector<std::string>::iterator it = files.begin();
                it != file.end(); ++it) {
            job.add_files(*it);
        }
    } else {
        std::cerr << "WARNING: no local file is specified" << std::endl;
    }
    if (vars_.count("cacheArchive")) {
        job.set_cache_archive(vars_["cacheArchive"].as<std::string>());
    }
    if ((it = conf_.find("mapred.input.split.size")) != conf_.end()) {
        job.set_split_size(boost::lexical_cast<int64_t>(it->second));
    } else {
        job.set_split_size(500l * 1024 * 1024); // default split size
    }
    // TODO fill job description
    return 0;
}*/

int Configuration::BuildJson(std::ostream& os) {
    rapidjson::Document doc(rapidjson::kObjectType);
    rapidjson::Document::AllocatorType& alloc = doc.GetAllocator();
    // Judge from -i parameter
    if (multivalue_.empty() && kv_.size() <= 2) {
        InteractiveGetConfig();
    }
    std::string result = GetConf("mapred.job.name");
    doc["name"].SetString(result.data(), result.size(), alloc);
    result = GetConf("cacheArchive");
    doc["cache_archive"].SetString(result.data(), result.size(), alloc);
    doc["split_size"] = boost::lexical_cast<int64_t>(GetConf(""));
    // TODO
    rapidjson::OStreamWrapper osw(os);
    rapidjson::Writer<rapidjson::OStreamWrapper> writer(osw);
    doc.Accept(writer);
    return 0;
}

std::string Configuration::Help() const {
    static const std::string help_text =
        "usage: shuttle command [options]\n\n"
        "command:\n"
        "    help                              show help information\n"
        "    streaming/bistreaming [flags]     start a computing job\n"
        "    update jobid [flags]              adjust the capacity of a phase\n"
        "    kill jobid [node task attempt]    cancel a job or a certain task\n"
        "    list [-a]                         get a list of current jobs\n"
        "    status jobid                      get details of a certain job\n"
        "    monitor jobid                     block and get job status periodically\n"
        "                                      until the job is finished\n\n"
        "options:\n"
        "    -h [ --help ]             show help information\n"
        "    -a [ --all ]              check all jobs including dead jobs in listing\n"
        "    -i [ --immediate ]        return immediately after submitting without monitoring\n"
        "    --input ARG               input file of a job\n"
        "    --output ARG              output path of a job\n"
        "    -f [ --file ] ARG         upload local file to computing node\n"
        "    --cacheArchive ARG\\#PATH  download file to certain path on computing node\n"
        "    --mappred ARG             command to map phase\n"
        "    --reducer ARG             command to reduce phase\n"
        "    --combiner ARG            command to combiner\n"
        "    --partitioner ARG         partitioner to divide intermediate output\n"
        "    --inputformat ARG         organization format of input\n"
        "    --outputformat ARG        organization format of output\n"
        "    --jobconf key=value       set the configuration of a job\n"
        "    --nexus ARG               comma splited server list of nexus\n"
        "    --nexus-file ARG          use flag file to get nexus server list\n"
        "    --nexus-root ARG          use this nexus root path to find master\n"
        "    --master ARG              use this master path in nexus to find master\n\n"
        "configuration:\n"
        "    mapred.job.name  name of the job\n"
        "    mapred.job.cpu.millicores  cpu occupation limit in millicores\n"
        "    mapred.job.memory.limit    memory occupation limit\n"
        "    mapred.job.map.capacity    max slot of map\n"
        "    mapred.job.reduce.capacity max slot of reduce\n"
    ;
    return help_text;
}

void Configuration::InteractiveGetConfig() {
}

}
}

