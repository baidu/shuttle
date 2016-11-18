#include "config.h"

#include <iostream>
#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/writer.h>
#include <cstdlib>

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
        ("command", po::value<std::string>(), "")
        ("subcommand", po::value< std::vector<std::string> >(), "");
    po::positional_options_description positional;
    positional.add("command", 1);
    positional.add("subcommand", -1);
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
        if (it->first == "input" || it->first == "files" || it->first == "subcommand") {
            multivalue_[it->first] = it->second.as< std::vector<std::string> >();
        } else if (it->first == "all" || it->first == "i" || it->first == "help") {
            kv_[it->first] = "true";
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
    // TODO subcommand is a vector
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

int Configuration::BuildJobDescription(sdk::JobDescription& job) {
    std::vector<std::string> strlist;
    std::string conf;
    GetConf("subcommand", strlist);
    if (!strlist.empty()) {
        if (strlist[0] == "streaming") {
            job.pipe_style = sdk::kStreaming;
        } else if (strlist[0] == "bistreaming") {
            job.pipe_style = sdk::kBiStreaming;
        } else {
            std::cerr << "ERROR: " << strlist[0]
                << " is not a valid job type" << std::endl;
            return -1;
        }
    } else {
        std::cerr << "ERROR: please define job type" << std::endl;
        return -1;
    }
    job.name = GetConf("mapred.job.name");
    if (job.name.empty()) {
        std::cerr << "ERROR: please offer the name of job" << std::endl;
        return -1;
    }
    GetConf("file", job.files);
    if (job.files.empty()) {
        std::cerr << "WARNING: no local file is specified" << std::endl;
    }
    job.cache_archive = GetConf("cacheArchive");
    conf = GetConf("mapred.input.split.size");
    if (!conf.empty()) {
        job.split_size = boost::lexical_cast<int64_t>(conf);
    } else {
        job.split_size = 500l * 1024 * 1024;
    }
    if (GetConf("command") == "legacy") {
        FillLegacyNodes();
    }
    if (nodes_.empty()) {
        std::cerr << "ERROR: no node configuration specified" << std::endl;
        return -1;
    }
    job.nodes = nodes_;
    job.map = successors_;
    return 0;
}

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
        "    legacy <pipe> [flags]             start a map-reduce job\n"
        "    dag <json> [file flags]           start a dag job\n"
        "    set <jobid> <node> <new capacity> adjust the capacity of a phase\n"
        "    kill <jobid> [node-task-attempt]  cancel a job or a certain task\n"
        "    list [-a]                         get a list of current jobs\n"
        "    status <jobid>                    get details of a certain job\n"
        "    monitor <jobid>                   block and get job status periodically\n"
        "                                      until the job is finished\n\n"
        "options:\n"
        "    -h [ --help ]             show help information\n"
        "    -a [ --all ]              check all jobs including dead jobs in listing\n"
        "    -i [ --immediate ]        return immediately without monitoring\n"
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
        "legacy configuration:\n"
        "    mapred.job.name  name of the job\n"
        "    mapred.job.cpu.millicores     cpu occupation limit in millicores\n"
        "    mapred.job.memory.limit       memory occupation limit\n"
        "    mapred.job.map.capacity       max slot of map\n"
        "    mapred.job.reduce.capacity    max slot of reduce\n"
        "    mapred.job.input.host         input dfs host\n"
        "    mapred.job.input.port         input dfs port\n"
        "    mapred.job.input.user         input dfs user\n"
        "    mapred.job.input.password     input dfs password\n"
        "    mapred.job.output.host        output dfs host\n"
        "    mapred.job.output.port        output dfs port\n"
        "    mapred.job.output.user        output dfs user\n"
        "    mapred.job.output.password    output dfs password\n"
        "    mapred.job.check.counters     enable counters function\n"
        "    mapred.map.tasks              numbers of map (deprecated)\n"
        "    mapred.map.max.attempts       max tolerant attempts of a map task\n"
        "    mapred.reduce.tasks           numbers of reduce\n"
        "    mapred.reduce.max.attempts    max tolerant attempts of a reduce task\n"
        "    mapred.ignore.map.failures    tolerance of failed map tasks\n"
        "    mapred.ignore.reduce.failures tolerance of fauled reduce tasks\n"
        "    mapred.decompress.input       decompress compressed input\n"
        "    mapred.output.compress        compress output data\n"
        "    mapred.input.split.size       block size to divide input data\n"
        "    map.key.field.separator       separator for key field in shuffling\n"
        "    num.key.fields.for.partition  fields to be used in partitioning\n"
        "    stream.num.map.output.key.fields\n"
        "            fields to be counted as key\n"
        "    mapred.map.tasks.speculative.execution\n"
        "            allow more than one attempt running at the same time\n"
        "    mapred.reduce.tasks.speculative.execution\n"
        "            allow more than one attempt running at the same time\n"
    ;
    return help_text;
}

int64_t Configuration::ParseMemory(const std::string& memory) {
    size_t dimension = memory.find_first_not_of("0123456789");
    int64_t base = boost::lexical_cast<int64_t>(memory.substr(0, dimension));
    for (size_t i = dimension; i < memory.size(); ++i) {
        switch (memory[i]) {
        case 'G':
        case 'g':
            base *= 1l << 30; break;
        case 'M':
        case 'm':
            base *= 1l << 20; break;
        case 'K':
        case 'k':
            base *= 1l << 10; break;
        default:
            return base;
        }
    }
    return base;
}

void Configuration::FillLegacyNodes() {
    nodes_.reserve(2);
    nodes_.resize(1);
    std::vector<std::string> strlist;
    std::string conf;
    sdk::NodeConfig* cur = &nodes_[0];
    cur->node = 1;
    cur->capacity = boost::lexical_cast<int32_t>(GetConf("mapred.job.map.capacity"));
    cur->total = boost::lexical_cast<int32_t>(GetConf("mapred.job.map.tasks"));
    cur->millicores = boost::lexical_cast<int32_t>(GetConf("mapred.job.cpu.millicores"));
    cur->memory = ParseMemory(GetConf("mapred.job.memory"));
    cur->command = GetConf("mapper");
    GetConf("input", strlist);
    if (strlist.empty()) {
        std::cerr << "ERROR: no input file specified" << std::endl;
        exit(-1);
    }
    sdk::DfsInfo info;
    info.host = GetConf("mapred.job.input.host");
    info.port = GetConf("mapred.job.input.port");
    info.user = GetConf("mapred.job.input.user");
    info.password = GetConf("mapred.job.input.password");
    for (std::vector<std::string>::iterator it = strlist.begin();
            it != strlist.end(); ++it) {
        info.path = *it;
        cur->inputs.push_back(info);
    }
    conf = GetConf("inputformat");
    if (boost::starts_with(conf, "Text")) {
        cur->input_format = sdk::kTextInput;
    } else if (boost::starts_with(conf, "Binary")) {
        cur->input_format = sdk::kBinaryInput;
    } else if (boost::starts_with(conf, "NLine")) {
        cur->input_format = sdk::kNLineInput;
    } else {
        cur->input_format = sdk::kTextInput;
    }
    conf = GetConf("partitioner");
    if (boost::iequals(conf, "keyfieldbased") ||
            boost::iequals(conf, "keyfieldbasedpartitioner")) {
        cur->partition = sdk::kKeyFieldBased;
    } else if (boost::iequals(conf, "inthash") ||
            boost::iequals(conf, "inthashpartitioner")) {
        cur->partition = sdk::kIntHash;
    } else {
        cur->partition = sdk::kKeyFieldBased;
    }
    cur->key_separator = GetConf("map.key.field.separator");
    cur->key_fields_num = boost::lexical_cast<int32_t>(
            GetConf("stream.num.map.output.key.fields"));
    cur->partition_fields_num = boost::lexical_cast<int32_t>(
            GetConf("num.key.fields.for.partition"));
    cur->allow_duplicates = boost::iequals("true",
            GetConf("mapred.map.tasks.speculative.execution"));
    cur->retry = boost::lexical_cast<int32_t>(GetConf("mapred.map.max.attempts"));
    cur->combiner = GetConf("combiner");
    cur->check_counters = boost::iequals("true",
            GetConf("mapred.job.check.counters"));
    cur->ignore_failures = boost::lexical_cast<int32_t>(
            GetConf("mapred.ignore.map.failures"));
    cur->decompress_input = boost::iequals("true",
            GetConf("mapred.decompress.input"));
    cur->compress_output = boost::iequals("true",
            GetConf("mapred.output.compress"));
    GetConf("cmdenv", cur->cmdenvs);
    info.path = GetConf("output");
    info.host = GetConf("mapred.job.output.host");
    info.port = GetConf("mapred.job.output.port");
    info.user = GetConf("mapred.job.output.user");
    info.password = GetConf("mapred.job.output.password");
    sdk::OutputFormat of;
    conf = GetConf("outputformat");
    if (boost::starts_with(conf, "Text")) {
        of = sdk::kTextOutput;
    } else if (boost::starts_with(conf, "Binary")) {
        of = sdk::kBinaryOutput;
    } else if (boost::starts_with(conf, "SuffixMultipleText")) {
        of = sdk::kSuffixMultipleTextOutput;
    } else {
        of = sdk::kTextOutput;
    }
    if (boost::lexical_cast<int32_t>(GetConf("mapred.job.reduce.tasks")) ||
            GetConf("reducer").empty()) {
        // map only
        cur->output = info;
        cur->output_format = of;
        return;
    }
    nodes_.resize(2);
    cur = &nodes_[1];
    cur->node = 2;
    cur->capacity = boost::lexical_cast<int32_t>(GetConf("mapred.job.reduce.capacity"));
    cur->total = boost::lexical_cast<int32_t>(GetConf("mapred.job.reduce.tasks"));
    cur->millicores = nodes_[0].millicores;
    cur->memory = nodes_[0].memory;
    cur->command = GetConf("reducer");
    cur->output = info;
    cur->output_format = of;
    cur->allow_duplicates = boost::iequals("true",
            GetConf("mapred.reduce.tasks.speculative.execution"));
    cur->retry = boost::lexical_cast<int32_t>(GetConf("mapred.reduce.max.attempts"));
    cur->check_counters = nodes_[0].check_counters;
    cur->ignore_failures = boost::lexical_cast<int32_t>(
            GetConf("mapred.ignore.reduce.failures"));
    cur->decompress_input = nodes_[0].decompress_input;
    cur->compress_output = nodes_[0].compress_output;
    cur->cmdenvs = nodes_[0].cmdenvs;
}

void Configuration::InteractiveGetConfig() {
}

}
}

