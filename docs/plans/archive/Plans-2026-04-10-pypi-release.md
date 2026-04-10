# TDXAPI PyPI 发布计划

创建日期: 2026-04-10

---

## Phase 1: 发布前准备

| Task | 内容 | DoD | Depends | Status |
|------|------|-----|---------|--------|
| 1.1 | 注册 PyPI 账户并获取 API Token | 在 https://pypi.org/manage/account/token/ 创建 token，权限 scope 为 "Entire account" 或指定项目 | - | cc:TODO |
| 1.2 | 配置 GitHub Secrets | 在仓库 Settings > Secrets and variables > Actions 中添加 `PYPI_API_TOKEN` | 1.1 | cc:完了 |
| 1.3 | 完善 pyproject.toml 元数据 | 添加 author、license、keywords、classifiers、project urls 等必要字段 | - | cc:完了 |
| 1.4 | 添加 LICENSE 文件 | 选择合适许可证（建议 MIT 或 Apache-2.0），创建 LICENSE 文件 | - | cc:完了 |
| 1.5 | 完善 README.md | 确保包含安装说明、基本用法、支持的 Python 版本 | - | cc:完了 |

## Phase 2: GitHub Actions 配置

| Task | 内容 | DoD | Depends | Status |
|------|------|-----|---------|--------|
| 2.1 | 创建发布工作流 | 创建 `.github/workflows/publish.yml`，支持手动触发 (workflow_dispatch) 和 tag 推送自动触发 | 1.2 | cc:完了 |
| 2.2 | 配置测试工作流 | 创建 `.github/workflows/test.yml`，在 PR 和 push 时自动运行测试 | - | cc:完了 |
| 2.3 | 验证工作流语法 | 使用 GitHub Actions 编辑器或 `act` 工具验证 YAML 语法正确 | 2.1, 2.2 | cc:完了 |

## Phase 3: 测试发布流程

| Task | 内容 | DoD | Depends | Status |
|------|------|-----|---------|--------|
| 3.1 | 本地构建测试 | 运行 `python -m build` 和 `twine check dist/*`，确保包可正常构建 | 1.3, 1.4, 1.5 | cc:完了 |
| 3.2 | 测试 PyPI 发布（可选） | 先发布到 https://test.pypi.org/ 验证流程 | 2.1 | cc:TODO |
| 3.3 | 创建测试 Tag 并推送 | 创建 `v0.1.0-test` tag，推送到 GitHub，验证 Actions 触发和运行 | 2.1, 2.3 | cc:TODO |

## Phase 4: 正式发布

| Task | 内容 | DoD | Depends | Status |
|------|------|-----|---------|--------|
| 4.1 | 确定版本号 | 更新 `pyproject.toml` 和 `__init__.py` 中的版本号为正式版本（如 0.1.0） | 3.3 | cc:完了 [797efd5] |
| 4.2 | 创建正式 Release | 在 GitHub 创建 Release，填写 changelog，系统自动发布到 PyPI | 4.1 | cc:完了 [449efac] |
| 4.3 | 验证 PyPI 发布 | 访问 https://pypi.org/project/ten2net-tdxapi/ 确认包已发布，可正常 `pip install` | 4.2 | cc:完了 |
| 4.4 | 在其他项目中测试安装 | 创建干净的虚拟环境，运行 `pip install ten2net-tdxapi` 并验证功能正常 | 4.3 | cc:完了 |

---

## 参考命令

```bash
# 本地构建测试
python -m pip install build twine
python -m build
twine check dist/*

# 手动测试发布到 TestPyPI
python -m twine upload --repository testpypi dist/*

# Tag 操作
git tag v0.1.0
git push origin v0.1.0
```

## 注意事项

1. **版本号管理**: 使用语义化版本 (SemVer)，格式为 `MAJOR.MINOR.PATCH`
2. **GitHub Actions 触发条件**: 
   - 推荐：推送 `v*` 标签时自动触发
   - 备选：手动触发 workflow_dispatch
3. **PyPI Token 权限**: 建议创建 project-specific token 而非全局 token
