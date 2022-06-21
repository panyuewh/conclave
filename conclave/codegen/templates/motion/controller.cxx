// MIT License
//
// Copyright (c) 2019 Oleksandr Tkachenko
// Cryptography and Privacy Engineering Group (ENCRYPTO)
// TU Darmstadt, Germany
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#include <cmath>
#include <fstream>
#include <iostream>
#include <random>
#include <regex>
#include <fmt/format.h>
#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>
#include "motioncore/base/party.h"
#include "motioncore/communication/communication_layer.h"
#include "motioncore/communication/tcp_transport.h"
#include "motioncore/statistics/analysis.h"
#include "motioncore/statistics/run_time_statistics.h"

#include "rapidcsv.h"
#include "workflow.h"

namespace po = boost::program_options;
namespace mo = encrypto::motion;

bool CheckPartyArgumentSyntax(const std::string& party_argument);

std::pair<po::variables_map, bool> ParseProgramOptions(int ac, char* av[]);

mo::PartyPointer CreateParty(const po::variables_map& user_options);

int main(int ac, char* av[]) 
{
  auto [user_options, help_flag] = ParseProgramOptions(ac, av);
  // if help flag is set - print allowed command line arguments and exit
  if (help_flag) return EXIT_SUCCESS;

  std::vector<std::vector<uint32_t>> input_rel;
  std::vector<std::vector<uint32_t>> output_rel;
  rapidcsv::Document input_doc(user_options["in_path"].as<std::string>());
  for (std::size_t idx=0; idx < input_doc.GetRowCount(); idx++) {
    auto record = input_doc.GetRow<uint32_t>(idx);
    input_rel.push_back(record);
  }

  mo::PartyPointer party{CreateParty(user_options)};

  auto statistics = EvaluateProtocol(party, input_rel, output_rel);

  rapidcsv::Document output_doc(user_options["out_path"].as<std::string>());
  output_doc.Clear(); 
  for (std::size_t idx = 0; idx < output_rel.size(); idx++)
    output_doc.InsertRow(idx, output_rel[idx]);
  output_doc.Save(); 

  mo::AccumulatedRunTimeStatistics accumulated_statistics;
  mo::AccumulatedCommunicationStatistics accumulated_communication_statistics;
  accumulated_statistics.Add(statistics);
  auto communication_statistics = party->GetCommunicationLayer().GetTransportStatistics();
  accumulated_communication_statistics.Add(communication_statistics);

  std::cout << mo::PrintStatistics(fmt::format("For Conclave"), accumulated_statistics,
                                   accumulated_communication_statistics);
  return EXIT_SUCCESS;
}

const std::regex kPartyArgumentRegex(
    "(\\d+),(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}),(\\d{1,5})");

bool CheckPartyArgumentSyntax(const std::string& party_argument) 
{
  // other party's id, IP address, and port
  return std::regex_match(party_argument, kPartyArgumentRegex);
}

std::tuple<std::size_t, std::string, std::uint16_t> ParsePartyArgument(
    const std::string& party_argument) 
{
  std::smatch match;
  std::regex_match(party_argument, match, kPartyArgumentRegex);
  auto id = boost::lexical_cast<std::size_t>(match[1]);
  auto host = match[2];
  auto port = boost::lexical_cast<std::uint16_t>(match[3]);
  return {id, host, port};
}

// <variables map, help flag>
std::pair<po::variables_map, bool> ParseProgramOptions(int ac, char* av[]) 
{
  using namespace std::string_view_literals;
  constexpr std::string_view kConfigFileMessage =
      "configuration file, other arguments will overwrite the parameters read from the configuration file";
  bool print, help;
  po::options_description description("Allowed options");
  
  // clang-format off
  description.add_options()
      ("help,h", po::bool_switch(&help)->default_value(false),"produce help message")
      ("disable-logging,l","disable logging to file")
      ("print-configuration,p", po::bool_switch(&print)->default_value(false), "print configuration")
      ("configuration-file,f", po::value<std::string>(), kConfigFileMessage.data())
      ("my-id", po::value<std::size_t>(), "my party id")
      //("my-id", po::value<std::size_t>()->default_value({{{PID}}}), "my party id")
      ("parties", po::value<std::vector<std::string>>()->multitoken(), 
                "info (id,IP,port) for each party e.g., --parties 0,127.0.0.1,23000 1,127.0.0.1,23001")
      //("parties", po::value<std::vector<std::string>>()->multitoken()->default_value(std::vector<std::string>(), { {{{IP_AND_PORTS}}} }), 
                //"info (id,IP,port) for each party e.g., --parties 0,127.0.0.1,23000 1,127.0.0.1,23001")
      ("in-path", po::value<std::string>(), "input data path")
      //("in-path", po::value<std::string>()->default_value("{{{INPUT_PATH}}}"), "input data path")
      ("out-path", po::value<std::string>(), "output data path");
      //("out-path", po::value<std::string>()->default_value("{{{OUTPUT_PATH}}}"), "output data path");
  // clang-format on

  po::variables_map user_options;

  po::store(po::parse_command_line(ac, av, description), user_options);
  po::notify(user_options);

  // argument help or no arguments (at least a configuration file is expected)
  if (user_options["help"].as<bool>() || ac == 1) 
  {
    std::cout << description << "\n";
    return std::make_pair<po::variables_map, bool>({}, true);
  }

  // read configuration file
  if (user_options.count("configuration-file")) 
  {
    std::ifstream ifs(user_options["configuration-file"].as<std::string>().c_str());
    po::variables_map user_option_config_file;
    po::store(po::parse_config_file(ifs, description), user_options);
    po::notify(user_options);
  }

  // print parsed parameters
  if (user_options.count("my-id")) 
  {
    if (print) std::cout << "My id " << user_options["my-id"].as<std::size_t>() << std::endl;
  } 
  else
    throw std::runtime_error("My id is not set but required");

  if (user_options.count("parties")) 
  {
    const std::vector<std::string> other_parties{
        user_options["parties"].as<std::vector<std::string>>()};
    std::string parties("Other parties: ");
    for (auto& p : other_parties) 
    {
      if (CheckPartyArgumentSyntax(p)) 
      {
        if (print) parties.append(" " + p);
      } 
      else 
      {
        throw std::runtime_error("Incorrect party argument syntax " + p);
      }
    }
    if (print) std::cout << parties << std::endl;
  } 
  else
    throw std::runtime_error("Other parties' information is not set but required");

  return std::make_pair(user_options, help);
}

mo::PartyPointer CreateParty(const po::variables_map& user_options) 
{
  const auto parties_string{user_options["parties"].as<const std::vector<std::string>>()};
  const auto number_of_parties{parties_string.size()};
  const auto my_id{user_options["my-id"].as<std::size_t>()};
  if (my_id >= number_of_parties) 
  {
    throw std::runtime_error(fmt::format(
        "My id needs to be in the range [0, #parties - 1], current my id is {} and #parties is {}",
        my_id, number_of_parties));
  }

  mo::communication::TcpPartiesConfiguration parties_configuration(number_of_parties);

  for (const auto& party_string : parties_string) 
  {
    const auto [party_id, host, port] = ParsePartyArgument(party_string);
    if (party_id >= number_of_parties) 
    {
      throw std::runtime_error(
          fmt::format("Party's id needs to be in the range [0, #parties - 1], current id "
                      "is {} and #parties is {}",
                      party_id, number_of_parties));
    }
    parties_configuration.at(party_id) = std::make_pair(host, port);
  }
  mo::communication::TcpSetupHelper helper(my_id, parties_configuration);
  auto communication_layer = std::make_unique<mo::communication::CommunicationLayer>
  (
      my_id, helper.SetupConnections()
  );
  auto party = std::make_unique<mo::Party>(std::move(communication_layer));
  auto configuration = party->GetConfiguration();
  // disable logging if the corresponding flag was set
  const auto logging{!user_options.count("disable-logging")};
  configuration->SetLoggingEnabled(logging);
  return party;
}
