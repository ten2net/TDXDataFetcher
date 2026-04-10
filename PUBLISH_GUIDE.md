# TDXAPI 发布到 PyPI 操作指南

本指南介绍如何将 TDXAPI 发布到 PyPI，以便其他项目可以通过 `pip install tdxapi` 安装使用。

---

## 📋 准备工作

### 步骤 1: 注册 PyPI 账户（任务 1.1）

1. 访问 https://pypi.org/account/register/ 注册账户
2. 完成邮箱验证
3. 登录后访问 https://pypi.org/manage/account/token/
4. 点击 "Add API token"
   - Token name: `tdxapi-github-actions`
   - Scope: 选择 "Entire account"（或限制到特定项目）
5. **复制生成的 token**（只显示一次，请妥善保存）

### 步骤 2: 配置 GitHub Secrets（任务 1.2）

1. 打开 GitHub 仓库页面: https://github.com/ten2net/tdxapi
2. 点击 Settings → Secrets and variables → Actions
3. 点击 "New repository secret"
   - Name: `PYPI_API_TOKEN`
   - Value: 粘贴步骤 1 中获取的 PyPI API Token
4. 点击 "Add secret"

---

## ✅ 自动完成的任务

以下任务已自动完成：

| 任务 | 状态 | 说明 |
|------|------|------|
| 1.3 完善 pyproject.toml | ✅ | 添加了 author、license、keywords、classifiers、project urls |
| 1.4 添加 LICENSE 文件 | ✅ | 创建了 MIT 许可证 |
| 1.5 完善 README.md | ✅ | README 已包含安装说明和基本用法 |
| 2.1 创建发布工作流 | ✅ | `.github/workflows/publish.yml` |
| 2.2 配置测试工作流 | ✅ | `.github/workflows/test.yml` |
| 2.3 验证工作流语法 | ✅ | YAML 语法验证通过 |
| 3.1 本地构建测试 | ✅ | `python -m build` 和 `twine check` 通过 |

---

## 🧪 可选：测试发布到 TestPyPI（任务 3.2）

在正式发布前，可以先发布到测试环境验证：

```bash
# 安装依赖
pip install build twine

# 构建包
python -m build

# 上传到 TestPyPI
twine upload --repository testpypi dist/*

# 测试安装
pip install --index-url https://test.pypi.org/simple/ tdxapi
```

---

## 🚀 正式发布（任务 4.x）

### 步骤 1: 确定版本号（任务 4.1）

当前版本: `0.1.0`

如需修改版本号，编辑以下文件：
- `pyproject.toml` 第 3 行: `version = "0.1.0"`
- `src/tdxapi/__init__.py` 第 5 行: `__version__ = "0.1.0"`

### 步骤 2: 创建并推送 Tag（任务 3.3 / 4.2）

```bash
# 确保在 main 分支且工作区干净
git checkout main
git status

# 提交所有更改（如果有）
git add .
git commit -m "Prepare for v0.1.0 release"

# 创建标签
git tag v0.1.0

# 推送标签到 GitHub（触发自动发布）
git push origin v0.1.0
```

**标签推送后将自动触发：**
1. GitHub Actions 运行测试
2. 构建 Python 包
3. 发布到 PyPI
4. 创建 GitHub Release

### 步骤 3: 验证 PyPI 发布（任务 4.3）

1. 访问 https://pypi.org/project/tdxapi/ 查看是否已发布
2. 等待 2-5 分钟后，测试安装：
   ```bash
   pip install tdxapi
   ```

### 步骤 4: 在其他项目中测试（任务 4.4）

```bash
# 创建干净环境测试
python3 -m venv test_env
source test_env/bin/activate  # Windows: test_env\Scripts\activate

# 安装
pip install tdxapi

# 测试导入
python3 -c "from tdxapi import TdxClient; print('安装成功')"
```

---

## 📁 自动创建的文件

已为您创建以下文件：

```
.
├── LICENSE                          # MIT 许可证
├── pyproject.toml                   # 已完善元数据
└── .github/
    └── workflows/
        ├── test.yml                 # PR/push 时自动测试
        └── publish.yml              # 发布到 PyPI
```

---

## 🔄 后续版本发布流程

发布新版本时，只需：

```bash
# 1. 更新版本号（pyproject.toml 和 __init__.py）
# 2. 提交更改
git add .
git commit -m "Bump version to v0.2.0"

# 3. 创建并推送新标签
git tag v0.2.0
git push origin v0.2.0

# 完成！GitHub Actions 会自动处理剩余步骤
```

---

## ❓ 常见问题

### Q: 发布失败，提示 "Invalid or non-existent authentication"
A: 检查 GitHub Secrets 中的 `PYPI_API_TOKEN` 是否正确配置

### Q: 如何删除已发布的版本？
A: PyPI 不允许删除已发布版本，只能创建新版本覆盖。建议先在 TestPyPI 测试

### Q: 可以手动触发发布吗？
A: 可以，访问 GitHub Actions → Publish to PyPI → Run workflow

---

## 📚 相关链接

- PyPI: https://pypi.org/project/tdxapi/
- GitHub: https://github.com/ten2net/tdxapi
- PyPI Token 管理: https://pypi.org/manage/account/token/
