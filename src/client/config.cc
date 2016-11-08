#include "config.h"

#include <iostream>

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
        ("input", po::value<std::string>(), "")
        ("output", po::value<std::string>(), "")
        ("file,f", po::value< std::vector<std::string> >(), "")
        ("cacheArchive", po::value<std::string>(), "")
        ("mapper", po::value<std::string>(), "")
        ("reducer", po::value<std::string>(), "")
        ("combiner", po::value<std::string>(), "")
        ("partitioner", po::value<std::string>(), "")
        ("inputformat", po::value<std::string>(), "")
        ("outputformat", po::value<std::string>(), "")
        ("jobconf", po::value< std::vector<std::string> >(jobconf), "")
        ("nexus", po::value<std::string>(), "")
        ("nexus-file", po::value<std::string>(), "")
        ("nexus-root", po::value<std::string>(), "")
        ("master", po::value<std::string>(), "")
        ("command", po::value<std::string>()->required(), "");
    po::positional_options_description positional;
    positional.add("command", 1);
    try {
        po::command_line_parser parser(argc, argv);
        po::store(parser.options(options).positional(positional).run(), vars_);
        po::notify(vars_);
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
        conf_[it->substr(0, colon)] = it->substr(colon + 1);
    }
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
        "    --cacheArchive ARG\\#PATH download file to certain path on computing node\n"
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
        "    --master ARG              use this master path in nexus to find master\n"
    ;
    return help_text;
}

int Configuration::BuildJobDescription(sdk::JobDescription& job) const {
    std::map<std::string, std::string>::iterator it;
    if ((it = conf_.find("mapred.job.name")) == conf_.end()) {
        std::cerr << "ERROR: please offer the name of job" << std::endl;
        return -1;
    }
    job.name = it->second;
    // TODO fill job description
    return 0;
}

}
}

