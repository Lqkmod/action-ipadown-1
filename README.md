# action-ipadown

Tải xuống phiên bản cũ của ứng dụng bằng Github Actions mà không cần máy tính!

**Warning: NOT supporting 2FA accounts currently**

## Use Cases

- Mua và tải xuống ứng dụng
- Tải xuống các phiên bản cũ của ứng dụng
- Lịch sử phiên bản truy vấn của ứng dụng
- Tra cứu thông tin ứng dụng dựa trên packageID hoặc appID

## Usage

1. Fork repo này
2. Bật Hành động trong repo được phân nhánh
   - Nhấp vào Tác vụ trên thanh công cụ
   - Nhấp vào `Tôi hiểu quy trình công việc của mình, hãy tiếp tục và kích hoạt chúng`
      ![image](https://user-images.githubusercontent.com/5344431/167505409-ef077008-2450-4e2d-9d43-2067244ac931.png)
3. Chạy Actions
  - Nhấp vào `IPA Down` trong danh sách bên trái:
      ![image](https://user-images.githubusercontent.com/5344431/167505630-1a741d9c-69de-470c-978e-1b8944dcfd3b.png)
  - Nhấp vào `Run workflow` ở bên phải:
      ![image](https://user-images.githubusercontent.com/5344431/167505748-52e0bba9-b9ec-44e1-9370-4452d3c3c66b.png)
  - Nhập ID Apple, Mật khẩu, Hoạt động và các thông số tương ứng (như packID)
  - Click `Run workflow`
  - Tìm tệp đầu ra trong cấu phần phần mềm Hành động hoặc Bản phát hành
      ![image](https://user-images.githubusercontent.com/5344431/167506938-c3e3529c-ee91-4661-a251-a12a2d0576cb.png)
  - Nếu các hành động không thành công tại `Setup iTunes Header Service`, vui lòng thử lại 1 ~ 2 lần trước khi gửi vấn đề

## Operations

Hiện có năm thao tác: tra cứu, máy chủ lịch sử, tải xuống, historyver_id, download_id

- tra cứu: thông tin truy vấn của ứng dụng dựa trên BundleID
- historyver: lịch sử truy vấn appVerID theo packID (với các truy vấn historyver_id theo appID)
- tải xuống: tải xuống IPA theo packID (download_id sử dụng appID) & appVerID (tùy chọn, mặc định cho phiên bản mới nhất)

Để sử dụng các thao tác này, hãy nhập thông tin ứng dụng của bạn như sau:
- Sử dụng ID gói:
  ![image](https://user-images.githubusercontent.com/5344431/167506427-1503417c-112f-4c45-b82b-7887f05b0dac.png)
- Sử dụng appID:
  ![image](https://user-images.githubusercontent.com/5344431/167506645-d29db175-ab38-45d3-b224-6cc94131e61d.png)
- Đồng thời cung cấp cho appVerID:
  ![image](https://user-images.githubusercontent.com/5344431/167506870-8dcaa565-3bd1-424e-a9d2-eed00bd4cffb.png)

## Credits

- ipatool-py: https://github.com/workflows-Ipa/ipatool-py
- actions-iTunes-header: https://github.com/workflows-Ipa/actions-iTunes-header
