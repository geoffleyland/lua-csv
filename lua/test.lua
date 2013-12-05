pcall(require, "strict")
local csv = require"csv"

local errors = 0

local function test(filename, correct_result, parameters)
  local result = {}
  local f = csv.open(filename)
  for r in f:lines() do
    result[#result+1] = table.concat(r, ",")
  end
  result = table.concat(result, "\n")
  if result ~= correct_result then
    io.stderr:write(
      ("Error reading %s.  Expected output\n%s\nActual output\n%s\n"):
      format(filename, correct_result, result))
    errors = errors + 1
  end
end

test("../test-data/embedded-newlines.csv", [[
embedded
newline,embedded
newline,embedded
newline
embedded
newline,embedded
newline,embedded
newline]])

if errors == 0 then
  io.stdout:write("Passed\n")
elseif errors == 1 then
  io.stdout:write("1 error\n")
else
  io.stdout:write(("%d errors\n"):format(errors))
end

os.exit(errors)