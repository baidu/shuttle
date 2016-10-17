//Copied from github.com/baidu/common, add SetMaxColWidth method
#ifndef  BAIDU_SHUTTLE_TPRINTER_H_
#define  BAIDU_SHUTTLE_TPRINTER_H_

#include <stdint.h>

#include <string>
#include <vector>


namespace baidu {
namespace shuttle {

using std::string;
class TPrinter {
public:
    typedef std::vector<string> Line;
    typedef std::vector<Line> Table;

    TPrinter();
    TPrinter(int cols);
    ~TPrinter();

    bool AddRow(const std::vector<string>& cols);

    bool AddRow(int argc, ...);

    bool AddRow(const std::vector<int64_t>& cols);

    void Print(bool has_head = true);

    string ToString(bool has_head = true);

    void Reset();

    void Reset(int cols);

    static string RemoveSubString(const string& input, const string& substr);

	void SetMaxColWidth(uint32_t width);

private:
	static const uint32_t kMaxColWidth = 50;
	uint32_t _max_col_width;
    size_t _cols;
    std::vector<int> _col_width;
    Table _table;
};

} // namespace shuttle
} // namespace baidu
#endif // BAIDU_SHUTTLE_TPRINTER_H_
