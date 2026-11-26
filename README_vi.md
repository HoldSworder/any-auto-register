# Any Auto Register

<p align="center">
  <a href="https://linux.do" target="_blank">
    <img src="https://img.shields.io/badge/LINUX-DO-FFB003?style=for-the-badge&logo=linux&logoColor=white" alt="LINUX DO" />
  </a>
</p>

> ⚠️ Tuyên bố miễn trừ trách nhiệm: Dự án này chỉ phục vụ mục đích học tập và nghiên cứu, không được sử dụng cho bất kỳ mục đích thương mại nào. Người sử dụng dự án này phải tự chịu hoàn toàn trách nhiệm về mọi hậu quả phát sinh.

Hệ thống quản lý và tự động đăng ký tài khoản đa nền tảng, hỗ trợ mở rộng dạng plugin, quản lý qua Web UI, đăng ký hàng loạt, đồng bộ trạng thái, và tự động khởi chạy Turnstile Solver cục bộ.

## Mục lục

- [Giao diện hiện tại & các nền tảng được hỗ trợ](#giao-diện-hiện-tại--các-nền-tảng-được-hỗ-trợ)
- [Tính năng](#tính-năng)
- [Sản phẩm tự vận hành](#sản-phẩm-tự-vận-hành)
- [Danh sách nhà tài trợ](#danh-sách-nhà-tài-trợ)
- [Xem trước giao diện](#xem-trước-giao-diện)
- [Công nghệ sử dụng](#công-nghệ-sử-dụng)
- [Yêu cầu môi trường](#yêu-cầu-môi-trường)
- [Tính năng chuyên biệt cho ChatGPT](#tính-năng-chuyên-biệt-cho-chatgpt)
- [Hỗ trợ dịch vụ email](#hỗ-trợ-dịch-vụ-email)
- [Bắt đầu nhanh](#bắt-đầu-nhanh)
- [Triển khai bằng Docker](#triển-khai-bằng-docker)
- [Plugin & phụ thuộc bên ngoài](#plugin--phụ-thuộc-bên-ngoài)
- [Xử lý sự cố thường gặp](#xử-lý-sự-cố-thường-gặp)
- [Cấu trúc dự án](#cấu-trúc-dự-án)
- [Hướng dẫn phát triển Electron](#hướng-dẫn-phát-triển-electron)
- [Nhóm thảo luận người dùng](#nhóm-thảo-luận-người-dùng)
- [Ủng hộ tác giả](#ủng-hộ-tác-giả)
- [Lịch sử Star](#lịch-sử-star)
- [Giấy phép](#giấy-phép)

## Giao diện hiện tại & các nền tảng được hỗ trợ

Theo mã nguồn frontend hiện tại, **các nền tảng hiển thị mặc định trong menu "Quản lý nền tảng"** bao gồm:

- ChatGPT
- iCloud (Hide My Email)

## Tính năng

- **Quản lý & đăng ký tài khoản đa nền tảng**: Danh sách tài khoản thống nhất, chi tiết, nhập, xuất nhiều định dạng, xóa, thao tác hàng loạt
- **iCloud Hide My Email**: Đăng nhập Apple ID (SRP + xác thực hai lớp, hoặc nhập cookie), tạo alias hàng loạt và xem hộp thư của alias
- **Nhiều chế độ thực thi**: Giao thức thuần, trình duyệt không giao diện (headless), trình duyệt có giao diện (headed)
- **Tích hợp nhiều dịch vụ email**: Tích hợp sẵn, bên thứ 3,自建 Worker Email và nhiều giải pháp khác
- **Hỗ trợ Captcha**: YesCaptcha, Turnstile Solver cục bộ (Camoufox)
- **Nhận mã SMS**: SmsBower / HeroSMS tự động thuê số và lấy mã OTP — hoàn toàn tự động khi ChatGPT yêu cầu add-phone
- **Hỗ trợ Proxy**: Luân phiên pool proxy, duy trì trạng thái proxy, tích hợp proxy trong quá trình đăng ký
- **Đăng ký hàng loạt**: Hỗ trợ cài đặt số lượng đăng ký, số lượng đồng thời, độ trễ khởi động giữa mỗi tài khoản
- **Log thời gian thực**: Xem log đăng ký trực tiếp trên frontend
- **Quản lý lịch sử tác vụ**: Xem lịch sử và xóa hàng loạt
- **Mở rộng dạng plugin**: Có thể tích hợp dịch vụ bên ngoài và quản lý độc lập

## Sản phẩm tự vận hành

Cảm ơn những sản phẩm tự vận hành đã hỗ trợ any-auto-register.

| Logo | Tên | Giới thiệu | Website |
| --- | --- | --- | --- |
| <a href="https://faka.gsyun.cloud/" target="_blank"><img src="frontend/public/logo.png" alt="阿晨小铺" width="140" /></a> | 阿晨小铺 | 本人经营,诚信稳定 | [https://faka.gsyun.cloud/](https://faka.gsyun.cloud/) |
| <a href="https://api.codelife.eu.cc/" target="_blank">zc-api</a> | zc-api | Dịch vụ trung chuyển cho các tình huống gọi mô hình như Claude Code và Codex. Băng thông 10Gbps đảm bảo phản hồi token đầu tiên nhanh hơn và kết nối ổn định. Cung cấp giao diện tin cậy cao, tích hợp dễ dàng và hỗ trợ giao hàng liên tục, phù hợp cho nhà phát triển và nhóm sử dụng lâu dài. Có hỗ trợ hóa đơn; truy cập trang chính thức để biết chi tiết. | [https://api.codelife.eu.cc/](https://api.codelife.eu.cc/) |

## Danh sách nhà tài trợ

Cảm ơn những người bạn và đối tác đã hỗ trợ any-auto-register.

| Logo | Tên | Giới thiệu | Website |
| --- | --- | --- | --- |
| <a href="https://www.ipipgo.com?inviteLink=umVuBvLV" target="_blank"><img src="docs/images/ipipgo.png" alt="IPIPGO" width="200" /></a> | IPIPGO | IPIPGO - Nhà cung cấp dịch vụ proxy IP toàn cầu với hơn 90 triệu IP sạch. Dịch vụ bao gồm IP động và tĩnh, SERP API và thu thập dữ liệu web. Đăng ký để được hỗ trợ kỹ thuật chuyên nghiệp 24/7. | [IPIPGO](https://www.ipipgo.com?inviteLink=umVuBvLV) |

## Xem trước giao diện

### Bảng điều khiển

![Bảng điều khiển](docs/images/dashboard.png)

### Cấu hình toàn cục / Quản lý plugin

![Cấu hình toàn cục / Quản lý plugin](docs/images/settings-integrations.png)

## Công nghệ sử dụng

| Tầng | Công nghệ |
| --- | --- |
| Backend | FastAPI + SQLite (SQLModel) |
| Frontend | React + TypeScript + Vite |
| HTTP | curl_cffi |
| Tự động hóa trình duyệt | Playwright / Camoufox |
| Giao thức đăng ký ChatGPT | Giao thức thuần; Sentinel PoW chạy trong sandbox Node |

## Yêu cầu môi trường

- Python 3.12+
- Node.js 18+ (vừa dùng để build frontend, vừa là runtime của bộ giải Sentinel PoW cho ChatGPT — **bắt buộc phải chạy được khi đăng ký**)
- Conda (khuyến nghị)
- Windows (khuyến nghị sử dụng script khởi động có sẵn trong repo)

## Tính năng chuyên biệt cho ChatGPT

Trong phiên bản hiện tại, **ChatGPT là một trong những nền tảng có chức năng hoàn thiện nhất**, không chỉ hỗ trợ đăng ký mà còn quản lý vòng đời Token, dò trạng thái và đồng bộ hệ thống bên ngoài.

### 1. Giao thức đăng ký

Toàn bộ luồng đăng ký ChatGPT nằm trong `platforms/chatgpt/protocol/`, là **triển khai giao thức thuần, không cần trình duyệt**:

- `http_client.py` — phiên làm việc với vân tay TLS dựa trên `curl_cffi`
- `auth_flow.py` — điều khiển máy trạng thái authorize của OpenAI (đăng ký, OTP, add-phone, Codex OAuth)
- `sentinel_quickjs.py` + `openai_sentinel_quickjs.js` — giải Sentinel PoW bằng cách chạy chính `sdk.js` của OpenAI trong sandbox Node

> ⚠️ **Sentinel cần runtime Node.** Token PoW tự chế vượt được lớp kiểm tra bề mặt ở `/sentinel/req`, nhưng dịch vụ gửi mã sẽ kiểm tra lại phía máy chủ, kết quả là email mã xác minh bị âm thầm loại bỏ — luồng nhìn có vẻ bình thường nhưng mã không bao giờ đến. Bắt buộc phải có `node` (>= 18) chạy được; nếu không nằm trong `PATH`, hãy đặt `OPENAI_SENTINEL_NODE_PATH` trỏ tới đường dẫn tuyệt đối.

Chỉ có ba điểm inject nằm trên tầng giao thức: adapter pool email (`protocol/mailbox_adapter.py`), controller nhận mã SMS (`services/sms_service.py`), và callback khi mật khẩu có hiệu lực. Cấu hình theo từng tác vụ được truyền qua tham số instance, **không ghi vào biến môi trường tiến trình**, nên nhiều tác vụ đăng ký chạy song song không ảnh hưởng lẫn nhau.

### 2. Chuyển đổi phương thức Token ChatGPT

Phiên bản hiện tại cung cấp hai chế độ đăng ký ChatGPT:

- **Có RT** (khuyến nghị mặc định)
  - Chạy đầy đủ luồng Codex OAuth
  - Xuất ra **Access Token + Refresh Token**
- **Không có RT**
  - Bỏ qua Codex OAuth (tiết kiệm khoảng 10 giây mỗi tài khoản)
  - Chỉ xuất ra **Access Token / Session**
  - Các chức năng phụ thuộc RT có thể không hoạt động

Chuyển đổi này có thể tìm thấy ở:

- Trang tác vụ đăng ký
- Cửa sổ popup đăng ký ChatGPT

### 3. Phương thức đăng ký ChatGPT

Cũng tại hai nơi đó, bạn có thể chọn danh tính dùng để đăng ký. Tùy chọn này kết hợp tự do với phương thức Token ở trên:

- **Đăng ký bằng email** (mặc định)
  - Lấy địa chỉ từ pool email và đọc mã gửi qua email
- **Đăng ký bằng số điện thoại**
  - Thuê số từ nền tảng nhận mã và đọc mã SMS, không tốn email
  - Tài khoản được liệt kê theo số điện thoại
- **Đăng ký bằng số điện thoại + liên kết email**
  - Đăng ký bằng số trước, sau đó liên kết một địa chỉ từ pool email và đọc một mã gửi qua email
  - Sau khi liên kết, tài khoản được liệt kê theo email, số điện thoại nằm trong chi tiết tài khoản

Hai phương thức dùng số điện thoại đều cần bật nhận mã và điền API Key tại **Cài đặt → 手机接码 (Nhận mã SMS)**, nếu không tác vụ sẽ báo lỗi ngay. Bước liên kết email chỉ được chấp nhận khi OpenAI còn giữ add-email trong luồng authorize hiện tại; nếu bị từ chối thì tài khoản vẫn được giữ lại và lý do được ghi trong chi tiết tài khoản để liên kết sau.

### 4. Liên kết 2FA TOTP

Trang tác vụ đăng ký và hộp thoại đăng ký ChatGPT còn có công tắc **绑定 2FA (Liên kết 2FA)**, **mặc định tắt**. Khi bật, tài khoản vừa đăng ký thành công sẽ được liên kết ngay một yếu tố thứ hai dạng TOTP:

- **Đường nhanh**: dùng lại chính phiên đăng ký để xin và kích hoạt khóa bí mật. Chuỗi đăng ký vừa xác minh xong vài chục giây trước nên máy chủ vẫn xem là "vừa xác thực" — không cần đăng nhập lại, không cần thêm email, xong trong vài giây.
- **Đường chậm**: chỉ chạy khi đường nhanh thất bại. Nó chạy lại toàn bộ chuỗi đăng nhập bằng email + mật khẩu rồi mới liên kết, tốn thêm một lần PoW và thường thêm một mã gửi qua email. Tài khoản dùng danh tính số điện thoại không có email để đăng nhập nên sẽ bỏ qua bước này.

Khóa bí mật được lưu cùng tài khoản (`totp_secret` trong `extra`) và có thể sao chép vào app xác thực từ ba chỗ: nhãn **2FA 已绑** trong danh sách, mục **复制 2FA 密钥 (Sao chép khóa 2FA)** trong menu thao tác, và ô khóa trong chi tiết tài khoản. Hộp thoại sau khi liên kết thủ công cũng hiển thị khóa kèm nút sao chép riêng, và khóa vẫn được in một lần trong nhật ký tác vụ. Cần lấy khóa hàng loạt thì dùng định dạng xuất kiểu `email----mật khẩu----2FA`.

Tài khoản cũ trong kho có thể liên kết riêng từ menu thao tác tài khoản (**绑定 2FA**), cũng thử dùng lại phiên trước rồi mới đăng nhập lại. Giống **补 RT (bù RT)**, thao tác này chạy như một tác vụ nền: cửa sổ nhật ký mở ra ngay để bạn thấy nó đi đường nào, có đang chờ mã qua email hay không, và có thể dừng giữa chừng. Khi thành công, khóa được in trong nhật ký và hiện lại ở khối có nút sao chép trên đầu cửa sổ.

> ⚠️ Máy chủ chỉ phát khóa bí mật đúng một lần và không API nào lấy lại được. Liên kết có hiệu lực ngay: mọi lần đăng nhập sau đó của tài khoản đều cần mã động. Các luồng bù RT và đăng nhập lại tự tính mã từ khóa đã lưu, nhưng mất khóa là mất luôn tài khoản.

### 5. Nhận mã SMS (add-phone)

OpenAI yêu cầu một số lượt đăng ký phải liên kết số điện thoại. Khi gặp add-phone, hệ thống tự thuê số, chờ SMS và gửi mã xác minh mà không cần thao tác thủ công. Cấu hình tại **Cài đặt → 手机接码 (Nhận mã SMS)**:

| Mục cấu hình | Mô tả |
| --- | --- |
| Bật nhận mã SMS | Khi tắt, add-phone quay về đường dẫn số thủ công (`OPENAI_PHONE_NUMBER`) |
| Nền tảng nhận mã | SmsBower / HeroSMS — cả hai dùng chung giao thức `handler_api.php` của sms-activate |
| API Key | Khóa nền tảng; kiểm tra ngay bằng nút "Kiểm tra số dư" |
| Mã dịch vụ | Nền tảng phân kho theo mã dịch vụ; OpenAI tương ứng với `dr` |
| ID quốc gia mặc định | Mặc định `52` (Thái Lan) |
| Tự chọn quốc gia tốt nhất | Chọn theo giá tăng dần kèm tồn kho; có thể giới hạn ứng viên bằng danh sách quốc gia cho phép |
| Tái sử dụng cùng số | Dùng lại số trong thời hạn thuê 20 phút cho đến khi đạt giới hạn số lần thành công mỗi số |
| Số giây chờ mỗi số / Số lần đổi số tối đa / Số lần thử lại mã trên mỗi số | Chiến lược thử lại khi không nhận được mã |

"Truy vấn xếp hạng quốc gia" liệt kê giá và tồn kho theo từng quốc gia cho mã dịch vụ đã chọn; thẻ màu xanh là các quốc gia OpenAI vẫn dùng SMS thuần.

> ⚠️ Từ năm 2025 OpenAI đã chuyển phần lớn quốc gia sang xác minh WhatsApp. Thực tế chỉ có **Thái Lan (country_id=52)** ổn định với SMS thuần. Các quốc gia khác có thể trả về số chỉ dùng WhatsApp và sẽ không nhận được SMS; chế độ tự chọn sẽ cảnh báo nhưng không chặn.

ID quốc gia, số giây chờ mỗi số và số lần đổi số tối đa còn có thể ghi đè theo từng tác vụ ở trang tác vụ đăng ký. Nền tảng và API Key chỉ được quản lý trong cấu hình toàn cục.

### 6. Đồng bộ trạng thái hàng loạt & re-upload cho ChatGPT

Ở đầu trang danh sách ChatGPT, hiện có hai loại chức năng hàng loạt:

- **Đồng bộ trạng thái**
  - Đồng bộ trạng thái tài khoản cục bộ đã chọn
  - Đồng bộ trạng thái CLIProxyAPI đã chọn
  - Hoặc thực hiện hàng loạt theo bộ lọc hiện tại
- **Re-upload những tài khoản không tìm thấy ở remote**
  - Re-upload auth-file không tìm thấy ở remote
  - Hỗ trợ "phạm vi lọc hiện tại" hoặc "tài khoản đã chọn hiện tại"

### 7. Xuất hàng loạt nhiều định dạng

Nút **导出 (Xuất)** ở góc trên bên phải danh sách tài khoản mở hộp thoại xuất: chọn phạm vi (các tài khoản đã tick, hoặc toàn bộ tài khoản khớp bộ lọc hiện tại — không phụ thuộc phân trang), chọn định dạng, xem trước rồi sao chép hoặc tải về.

| Định dạng | Một dòng trông như thế nào |
| --- | --- |
| `email_pw` | `email----mật khẩu` |
| `email_pw_2fa` | `email----mật khẩu----khóa 2FA` |
| `email_pw_2fa_at` | thêm `----AccessToken` |
| `email_pw_2fa_rt` | thêm `----RefreshToken` |
| `email_pw_2fa_at_rt` | đủ mọi thông tin đăng nhập và gọi API trên một dòng |
| `email_pw_2fa_phone` | thêm `----số điện thoại` |
| `email_pw_rt` | `email----mật khẩu----RefreshToken` |
| `email_2fa` | `email----khóa 2FA` |
| `at` / `rt` / `totp` | mỗi dòng một token; tài khoản thiếu trường đó sẽ bị bỏ qua |
| `csv` / `json` | đầy đủ trường, dùng cho Excel hoặc script |

Trường rỗng vẫn giữ dấu phân cách (dấu cuối trong `a@b.com----pw----` không bị bỏ), nên script cắt theo `----` không bao giờ lệch cột. Danh mục định dạng nằm ở `EXPORT_FORMATS` trong `services/account_export.py`; thêm định dạng mới chỉ sửa một chỗ, dropdown ở frontend tự cập nhật.

## Hỗ trợ dịch vụ email

Theo cấu hình thực tế trong trang đăng ký, dự án hỗ trợ các dịch vụ email sau:

| Tên dịch vụ | Định danh | Ghi chú |
| --- | --- | --- |
| LuckMail | `luckmail` | Có thể đăng ký miễn phí để test, **check-in hàng ngày để tiếp tục nhận email** |
| MoeMail | `moemail` | Phương án mặc định phổ biến, tự động đăng ký tài khoản và tạo email |
| TempMail.lol | `tempmail_lol` | Email tạm thời, một số khu vực có thể cần proxy |
| SkyMail (CloudMail) | `skymail` | Sử dụng qua API / Token / Domain |
| YYDS Mail / MaliAPI | `maliapi` | Hỗ trợ chiến lược domain tự động |
| GPTMail | `gptmail` | Tạo email tạm thời qua GPTMail API và xoay vòng, hỗ trợ ghép ngẫu nhiên khi đã biết domain |
| DuckMail | `duckmail` | Email tạm thời |
| Freemail | `freemail` | Dịch vụ email tự xây dựng |
| Laoudo | `laoudo` | Email cố định |
| CF Worker | `cfworker` |自建 email qua Cloudflare Worker |

### Ghi chú về iCloud Hide My Email

Nền tảng iCloud không dùng pool email tạm thời ở bảng trên, mà dùng chính địa chỉ Hide My Email của Apple:

1. Thêm tài khoản Apple ID chính trong "iCloud Hide My Email → Quản lý tài khoản", theo một trong hai cách:
   - **Đăng nhập bằng mật khẩu**: chạy giao thức SRP của Apple, hoàn tất xác thực hai lớp khi cần (đẩy tới thiết bị tin cậy hoặc mã SMS);
   - **Nhập cookie**: dán cookie iCloud xuất từ trình duyệt.
2. Sau khi đăng nhập, tạo alias hàng loạt trong tab "Alias". Apple giới hạn **tối đa 5 alias mỗi giờ**, backend sẽ tự giới hạn theo từng tài khoản chính.
3. Mật khẩu IMAP của tài khoản (mật khẩu dành riêng cho ứng dụng) được dùng để đọc hộp thư alias. Mọi thông tin đăng nhập đều được mã hóa AES-256-GCM trước khi lưu.

Khóa mã hóa lấy từ biến môi trường `CREDENTIAL_ENCRYPTION_KEY`; nếu chưa đặt, hệ thống tự tạo khóa cục bộ tại `.secrets/credential_key`.

## Bắt đầu nhanh

### 1. Tạo và kích hoạt môi trường Conda

```bash
conda create -n any-auto-register python=3.12 -y
conda activate any-auto-register
```

### 2. Cài đặt phụ thuộc backend

```bash
pip install -r requirements.txt
```

### 3. Cài đặt phụ thuộc trình duyệt

```bash
python -m playwright install chromium
python -m camoufox fetch
```

### 4. Cài đặt và build frontend

```bash
cd frontend
npm install
npm run build
cd ..
```

Sau khi build xong, tài nguyên tĩnh được xuất ra:

```text
./static
```

### 5. Khởi động dự án

#### Khuyến nghị cho Windows

PowerShell:

```powershell
.\start_backend.ps1
```

CMD:

```bat
start_backend.bat
```

#### Khởi động thủ công

```bash
conda activate any-auto-register
python main.py
```

Sau khi khởi động, truy cập mặc định:

```text
http://localhost:8000
```

> Nếu đã thực hiện `npm run build`, frontend sẽ do FastAPI trực tiếp quản lý, do đó truy cập qua `8000`, không phải `5173`.

## Script khởi động Windows

Repo đã cung cấp các script sau:

- `start_backend.bat`
- `start_backend.ps1`
- `stop_backend.bat`
- `stop_backend.ps1`

Các script này bắt buộc sử dụng môi trường `any-auto-register` để khởi động/dừng backend, tránh các vấn đề thường gặp:

- Backend khởi động được nhưng Solver không chạy
- `ModuleNotFoundError: quart`
- Turnstile Solver trên frontend luôn hiển thị "Chưa chạy"

Khi dừng dịch vụ, thực thi:

PowerShell:

```powershell
.\stop_backend.ps1
```

CMD:

```bat
stop_backend.bat
```

Mặc định sẽ dừng:

- Cổng backend: `8000`
- Cổng Solver: `8889`

## Chế độ phát triển Frontend

Phù hợp khi cần debug giao diện React.

### Terminal 1: Khởi động backend

```powershell
.\start_backend.ps1
```

### Terminal 2: Khởi động Vite

```bash
cd frontend
npm run dev
```

Truy cập:

```text
http://localhost:5173
```

Vite sẽ proxy các request `/api` về backend `http://localhost:8000`.

## Giải thích về Turnstile Solver

### Tự động khởi động

Turnstile Solver cục bộ sẽ tự động chạy khi FastAPI backend khởi động, mặc định tại:

```text
http://localhost:8889
```

Frontend "Cấu hình toàn cục → Captcha → Turnstile Solver" hiển thị **kết quả dò từ backend**, do đó:

- Backend chưa khởi động → Frontend hiển thị "Chưa chạy"
- Backend đã khởi động nhưng không đúng môi trường conda → Solver có thể không khởi động được

### Khởi động Solver thủ công

```bash
conda activate any-auto-register
python services/turnstile_solver/start.py --browser_type camoufox --port 8889
```

### Log Solver

Nếu khởi động thất bại, xem:

```text
services/turnstile_solver/solver.log
```

## Triển khai bằng Docker

Thư mục gốc repo đã cung cấp:

- `Dockerfile`
- `docker-compose.yml`

Nội dung triển khai mặc định bao gồm:

- FastAPI Backend
- Tài nguyên tĩnh frontend đã build
- Persist thư mục database SQLite `./data`
- Turnstile Solver tự động khởi chạy kèm backend

### Khởi động

```bash
docker compose up -d --build
```

Lần build đầu tiên sẽ tải thêm phụ thuộc Python, Playwright Chromium và Camoufox, do đó thời gian sẽ lâu hơn đáng kể.

Dockerfile hiện tại đã được sửa để cài Camoufox qua link trực tiếp, tránh giới hạn ẩn danh khi truy cập GitHub Releases API.

### Truy cập

```text
http://localhost:8000
```

### Dừng

```bash
docker compose down
```

### Xem log

```bash
docker compose logs -f app
```

### Persist dữ liệu

Container mặc định sử dụng:

```text
DATABASE_URL=sqlite:////app/data/account_manager.db
```

Host machine sẽ mount vào:

```text
./data
```

### Biến môi trường thường dùng

| Biến | Mặc định | Ghi chú |
| --- | --- | --- |
| `HOST` | `0.0.0.0` | Địa chỉ lắng nghe FastAPI |
| `PORT` | `8000` | Cổng lắng nghe FastAPI |
| `DATABASE_URL` | `sqlite:////app/data/account_manager.db` | Đường dẫn database SQLite |
| `APP_ENABLE_SOLVER` | `1` | Có tự động khởi chạy Solver không, đặt `0` để tắt |
| `SOLVER_PORT` | `8889` | Cổng lắng nghe Solver |
| `LOCAL_SOLVER_URL` | `http://127.0.0.1:8889` | Địa chỉ backend truy cập Solver |
| `OPENAI_SENTINEL_NODE_PATH` | `node` | File thực thi Node dùng cho bộ giải Sentinel PoW; điền đường dẫn tuyệt đối khi `node` không nằm trong `PATH` |

Nếu cần thay đổi cấu hình như `OPENAI_*`, chỉ cần ghi vào file `.env` ở thư mục gốc repo, `docker compose` sẽ tự động inject vào môi trường container.

### Tham số build Camoufox

Nếu cần ghi đè phiên bản upstream, có thể chỉ định khi build:

```bash
CAMOUFOX_VERSION=135.0.1 CAMOUFOX_RELEASE=beta.24 docker compose build app
```

### Khuyến nghị sử dụng Docker

- Image hiện tại chủ yếu bao phủ ứng dụng chính và Turnstile Solver cục bộ
- Logic tự động cài đặt/khởi chạy `CLIProxyAPI` vẫn thiên về môi trường host machine
- Nếu phụ thuộc vào `conda`, Go hoặc file thực thi Windows, không khuyến nghị chạy trực tiếp trong Linux container hiện tại
- Nếu chỉ cần Web UI, quản lý tài khoản, điều phối tác vụ và Solver cục bộ, cấu hình Compose hiện tại có thể sử dụng trực tiếp

## Plugin & phụ thuộc bên ngoài

### Nguồn email tạm thời

Dự án hỗ trợ自建 email tạm thời qua Cloudflare Worker, nguồn giải pháp từ:

- <https://github.com/dreamhunter2333/cloudflare_temp_email>

### Địa chỉ Git plugin bên ngoài

Dự án hiện hỗ trợ cài đặt/khởi động các thành phần bên ngoài sau:

| Dự án | Mục đích | Địa chỉ Git |
| --- | --- | --- |
| CLIProxyAPI | Dịch vụ quản lý CPA / Proxy Pool | `https://github.com/router-for-me/CLIProxyAPI.git` |

Nút **"Cài đặt phiên bản mới nhất / Cập nhật lên phiên bản mới nhất"** trong trang plugin sẽ đồng bộ code mới nhất từ repo, và đã hỗ trợ **gỡ cài đặt** (sẽ dừng dịch vụ trước, sau đó xóa thư mục plugin cục bộ).
Mặc định cập nhật theo **semver tag mới nhất**; cũng có thể chuyển về chế độ **branch HEAD** trong "Cài đặt → Plugin → Chiến lược cài đặt/cập nhật".

Nếu cần thay đổi địa chỉ `ghproxy`, `gitclone`, mirror Git doanh nghiệp hoặc proxy khác, cần đồng bộ sửa đổi:

```text
services/external_apps.py
```

## Xử lý sự cố thường gặp

### 1. Turnstile Solver hiển thị "Chưa chạy" trên frontend

Trước tiên kiểm tra backend đã khởi động chưa:

```bash
curl http://localhost:8000/api/solver/status
```

Kết quả trả về bình thường:

```json
{"running":true}
```

Nếu không truy cập được cổng `8000`, vấn đề nằm ở backend, không phải Solver.

### 2. Lỗi `ModuleNotFoundError: quart`

Python khởi động backend hiện tại không phải môi trường `any-auto-register`, vui lòng sử dụng:

```powershell
.\start_backend.ps1
```

hoặc:

```bat
start_backend.bat
```

### 3. Xác nhận Python hiện tại có chính xác không

```bash
python -c "import sys; print(sys.executable)"
```

Kết quả mong đợi:

```text
D:\miniconda\conda3\envs\any-auto-register\python.exe
```

### 4. Solver mở được nhưng trạng thái vẫn bất thường

Kiểm tra hai địa chỉ sau:

```text
http://localhost:8000/api/solver/status
http://localhost:8889/
```

Nếu địa chỉ thứ hai mở được nhưng địa chỉ thứ nhất không thông, vấn đề nằm ở backend, không phải Solver.

### 5. Cổng bị chiếm dụng

Nếu khởi động báo lỗi `WinError 10048`, trước tiên thực thi:

```powershell
.\stop_backend.ps1
```

Sau đó khởi động lại:

```powershell
.\start_backend.ps1
```

### 6. Đăng ký ChatGPT không nhận được mã xác minh

Trước tiên hãy xác nhận `node` chạy được:

```bash
node --version
```

Bộ giải Sentinel PoW chạy `sdk.js` của OpenAI trong sandbox Node. Không có Node thì token tính ra sẽ không vượt được lớp kiểm tra lại phía máy chủ, và **email mã xác minh bị âm thầm loại bỏ** — log không báo lỗi rõ ràng nhưng mã không bao giờ đến. Nếu `node` không nằm trong `PATH`, hãy trỏ `OPENAI_SENTINEL_NODE_PATH` tới đường dẫn tuyệt đối.

## Cấu trúc dự án

```text
any-auto-register/
├── api/
├── core/
├── docs/
├── electron/
├── frontend/
├── platforms/
├── services/
│   ├── solver_manager.py
│   └── turnstile_solver/
├── static/
├── tests/
├── main.py
├── requirements.txt
├── docker-compose.yml
├── Dockerfile
├── start_backend.bat
├── start_backend.ps1
├── stop_backend.bat
└── stop_backend.ps1
```

## Hướng dẫn phát triển Electron

Chế độ phát triển Electron sẽ không tự động khởi động Python backend.

Vui lòng khởi động backend tại thư mục gốc dự án trước:

```powershell
.\start_backend.ps1
```

Sau đó mới chạy Electron.

## Nhóm thảo luận người dùng

- Nhóm QQ: **1065114376** (Nhóm thảo luận người dùng đăng ký any-auto-register)

## Ủng hộ tác giả

Nếu dự án này hữu ích với bạn, vui lòng ủng hộ để tác giả tiếp tục duy trì và cập nhật dự án.

![Mã ủng hộ](docs/images/dashang.JPG)

## Lịch sử Star

<a href="https://star-history.dera.page/#zc-zhangchen/any-auto-register&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://star-history.dera.page/image?repos=zc-zhangchen/any-auto-register&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://star-history.dera.page/image?repos=zc-zhangchen/any-auto-register&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://star-history.dera.page/image?repos=zc-zhangchen/any-auto-register&type=date&legend=top-left" />
 </picture>
</a>

## Giấy phép

Giấy phép MIT — Chỉ phục vụ mục đích học tập và nghiên cứu, nghiêm cấm sử dụng cho mục đích thương mại.
