#ifndef  _BAIDU_SHUTTLE_TABLE_PRINTER_H_
#define  _BAIDU_SHUTTLE_TABLE_PRINTER_H_
#include <string>
#include <vector>
#include <stdint.h>

namespace baidu {
namespace shuttle {

class TPrinter {
public:
    typedef std::vector<std::string> Line;
    typedef std::vector<Line> Table;

    TPrinter();
    TPrinter(int cols);
    ~TPrinter();

    bool AddRow(const std::vector<std::string>& cols);

    bool AddRow(int argc, ...);

    bool AddRow(const std::vector<int64_t>& cols);

    void Print(bool has_head = true);

    std::string ToString(bool has_head = true);

    void Reset();

    void Reset(int cols);

    static std::string RemoveSubString(const std::string& input, const std::string& substr);

    void SetMaxColWidth(uint32_t width);

private:
    static const uint32_t kMaxColWidth = 50;
    uint32_t _max_col_width;
    size_t _cols;
    std::vector<int> _col_width;
    Table _table;
};

}
}

#endif

