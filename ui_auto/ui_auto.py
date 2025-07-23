# 实现windows应用的UI自动化
import logging
import json
from pywinauto import application, Desktop, findwindows
from pywinauto.controls.uiawrapper import UIAWrapper
from dataclasses import dataclass
from typing import Optional, List, Union, Dict
import re

@dataclass
class ControlSelector:
    """控件选择器结构体"""
    title: Optional[str] = None
    auto_id: Optional[str] = None  
    control_id: Optional[int] = None
    class_name: Optional[str] = None
    control_type: Optional[str] = None
    found_index: Optional[int] = None  # 如果有多个匹配项，指定使用第几个（从0开始）

    def __eq__(self, value):
        if not isinstance(value, ControlSelector):
            return NotImplemented
        return (self.title == value.title and
                self.auto_id == value.auto_id and
                self.control_id == value.control_id and
                self.class_name == value.class_name and
                self.control_type == value.control_type)

    def __hash__(self):
        return hash((self.title, self.auto_id, self.control_id, self.class_name, self.control_type))

def dump_control_selectors(selectors: List[ControlSelector]) -> str:
    return json.dumps([s.__dict__ for s in selectors], ensure_ascii=False, indent=4)

def load_control_selectors(str: str) -> List[ControlSelector]:
    """从字符串加载控件选择器列表"""
    try:
        data = json.loads(str)
        return [ControlSelector(**item) for item in data]
    except json.JSONDecodeError as e:
        logging.error(f"加载控件选择器失败: {str(e)}")
        return []

class UIAuto:
    def __init__(self):
        self.app = None
        self.main_window = None
    
    def connect(self, title_re=None, process_id=None, executable=None):
        """连接到已有的应用程序"""
        try:
            if title_re:
                self.app = application.Application().connect(title_re=title_re)
            elif process_id:
                self.app = application.Application().connect(process=process_id)
            elif executable:
                self.app = application.Application().connect(path=executable)
            else:
                raise ValueError("必须提供 title_re, process_id 或 executable 参数")
            
            return True
        except Exception as e:
            logging.error(f"连接失败: {str(e)}")
            return False

    def save_controls_to_file(self, filename='controls.json'):
        # 打印所有窗口
        control_infos = {}
        windows = self.app.windows()
        for window in windows:
            control_info = self.traverse_controls(window)
            control_infos[window.window_text()] = control_info
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(control_infos, f, ensure_ascii=False, indent=4)
        return control_infos

    def traverse_controls(self, parent_control, max_depth=100, current_depth=0):
        """递归遍历控件树，并将控件详情和层级信息保存在字典中。不允许字符串/数字以外的类型"""
        
        if not parent_control or current_depth >= max_depth:
            return {}
        
        try:
            # 获取当前控件的基本信息
            control_info = {
                'depth': current_depth,
                'window_text': str(parent_control.window_text()) if parent_control.window_text() else '',
                'class_name': str(parent_control.class_name()) if hasattr(parent_control, 'class_name') else '',
                'control_type': str(parent_control.control_type()) if hasattr(parent_control, 'control_type') else '',
                'auto_id': str(parent_control.automation_id()) if hasattr(parent_control, 'automation_id') else '',
                'control_id': int(parent_control.control_id()) if hasattr(parent_control, 'control_id') and parent_control.control_id() else 0,
                'rectangle': str(parent_control.rectangle()) if hasattr(parent_control, 'rectangle') else '',
                'is_enabled': bool(parent_control.is_enabled()) if hasattr(parent_control, 'is_enabled') else False,
                'is_visible': bool(parent_control.is_visible()) if hasattr(parent_control, 'is_visible') else False,
                'children': []
            }
            
            # 获取子控件
            try:
                children = parent_control.children()
                for child in children:
                    control_info['children'].append(self.traverse_controls(child, max_depth, current_depth + 1))
            except Exception as e:
                logging.debug(f"获取子控件失败: {str(e)}")
            
            return control_info
            
        except Exception as e:
            logging.error(f"遍历控件失败 (深度 {current_depth}): {str(e)}")
            return {
                'depth': current_depth,
                'error': str(e),
                'children': []
            }

    def find_control_path(self, target_selector: ControlSelector, start_window=None) -> Optional[List[ControlSelector]]:
        """通过ControlSelector获取一个匹配的控件路径，如果找到了返回控件路径"""
        if not self.app:
            logging.error("未连接到应用程序")
            return None
        
        try:
            # 如果没有指定起始窗口，则从所有窗口开始搜索
            if start_window is None:
                windows = self.app.windows()
                selector_dict = {}
                for window in windows:
                    path = self._search_control_in_tree(window, target_selector, [], selector_dict)
                    if path:
                        return path
                return None
            else:
                return self._search_control_in_tree(start_window, target_selector, [], {})

        except Exception as e:
            logging.error(f"查找控件路径失败: {str(e)}")
            return None
    
    def _search_control_in_tree(self, control, target_selector: ControlSelector, current_path: List[ControlSelector], current_level_selector_dict: Dict[ControlSelector, int]) -> Optional[List[ControlSelector]]:
        """在控件树中递归搜索匹配的控件"""
        try:
            # 创建当前控件的选择器
            current_selector = ControlSelector(
                title=control.window_text() if control.window_text() else None,
                auto_id=control.automation_id() if hasattr(control, 'automation_id') and control.automation_id() else None,
                control_id=control.control_id() if hasattr(control, 'control_id') and control.control_id() else None,
                class_name=control.class_name() if hasattr(control, 'class_name') and control.class_name() else None,
                control_type=control.control_type() if hasattr(control, 'control_type') and control.control_type() else None
            )
            if current_selector not in current_level_selector_dict:
                current_level_selector_dict[current_selector] = 0
            else:
                current_level_selector_dict[current_selector] += 1
            current_selector.found_index = current_level_selector_dict[current_selector]

            # 检查当前控件是否匹配目标选择器
            if self._is_control_match(current_selector, target_selector):
                return current_path + [current_selector]
            
            # 递归搜索子控件
            try:
                children = control.children()
                selector_dict = {}
                for child in children:
                    result = self._search_control_in_tree(child, target_selector, current_path + [current_selector], selector_dict)
                    if result:
                        return result
            except Exception as e:
                logging.debug(f"获取子控件失败: {str(e)}")
            
            return None
            
        except Exception as e:
            logging.debug(f"搜索控件时出错: {str(e)}")
            return None
    
    def _is_control_match(self, current_selector: ControlSelector, selector: ControlSelector) -> bool:
        """检查控件是否匹配选择器条件"""
        try:
            # 检查每个非空的选择器条件
            if selector.title is not None:
                if current_selector.title != selector.title:
                    return False
            
            if selector.auto_id is not None:
                if current_selector.auto_id != selector.auto_id:
                    return False
            
            if selector.control_id is not None:
                if current_selector.control_id != selector.control_id:
                    return False
            
            if selector.class_name is not None:
                if current_selector.class_name != selector.class_name:
                    return False
            
            if selector.control_type is not None:
                if current_selector.control_type != selector.control_type:
                    return False

            if selector.found_index is not None:
                # 如果指定了found_index，则需要检查当前控件在同类控件中的索引
                if current_selector.found_index != selector.found_index:
                    return False
            
            return True
            
        except Exception as e:
            logging.debug(f"匹配控件时出错: {str(e)}")
            return False

    def get_control(self, selector: ControlSelector):
        """使用ControlSelector直接定位app中的某一个控件"""
        if not self.app:
            logging.error("未连接到应用程序")
            return None
        
        try:
            # 构建查找参数
            kwargs = {}
            if selector.title is not None:
                kwargs['title'] = selector.title
            if selector.auto_id is not None:
                kwargs['auto_id'] = selector.auto_id
            if selector.control_id is not None:
                kwargs['control_id'] = selector.control_id
            if selector.class_name is not None:
                kwargs['class_name'] = selector.class_name
            if selector.control_type is not None:
                kwargs['control_type'] = selector.control_type
            
            if not kwargs:
                logging.error("ControlSelector没有提供任何选择条件")
                return None
            
            # 如果指定了found_index，则获取指定索引的控件
            if selector.found_index is not None:
                kwargs['found_index'] = selector.found_index
            
            # 在所有窗口中查找控件
            control = self.app.window(**kwargs)
            
            logging.info(f"成功找到控件: {control.window_text()}")
            return control
            
        except Exception as e:
            logging.error(f"定位控件失败: {str(e)}, 查找条件: {kwargs}")
            return None

    def get_control_by_path(self, window, path: List[ControlSelector]):
        """根据控件选择器路径获取控件"""
        try:
            current_control = window

            for i, selector in enumerate(path):
                # 构建child_window的查找参数
                kwargs = {}
                if selector.title is not None:
                    kwargs['title'] = selector.title
                if selector.auto_id is not None:
                    kwargs['auto_id'] = selector.auto_id
                if selector.control_id is not None:
                    kwargs['control_id'] = selector.control_id
                if selector.class_name is not None:
                    kwargs['class_name'] = selector.class_name
                if selector.control_type is not None:
                    kwargs['control_type'] = selector.control_type
                
                if not kwargs:
                    logging.error(f"路径第 {i} 项没有提供任何选择条件")
                    return None
                
                kwargs['visible_only'] = False  # 不限制可见性

                try:
                    if selector.found_index is not None:
                        kwargs['found_index'] = selector.found_index
                        if current_control is None:
                            current_control = self.app.window(**kwargs)
                        else:
                            current_control = current_control.child_window(**kwargs)
                    else:
                        if current_control is None:
                            current_control = self.app.window(**kwargs)
                        else: # 默认获取第一个匹配项
                            current_control = current_control.child_window(**kwargs)
                    
                    logging.info(f"第 {i} 层找到控件: {current_control.window_text()}, {current_control.class_name()}, {current_control.control_id()}")
                    
                except Exception as e:
                    logging.error(f"在第 {i} 层查找控件失败: {str(e)}, 查找条件: {kwargs}")
                    return None
            
            logging.info(f"成功找到目标控件: {current_control.window_text()}")
            return current_control
        except Exception as e:
            logging.error(f"根据路径获取控件失败: {str(e)}")
            return None
        
    
    def click(self, control, button='left'):
        if not control:
            return False
        
        try:
            if button == 'left':
                control.click()
            elif button == 'right':
                control.right_click()
            elif button == 'double':
                control.double_click()
            logging.info(f"点击成功: {control.window_text()}")
            return True
        except Exception as e:
            logging.error(f"点击失败: {str(e)}")
            return False
    
    def type_text(self, control, text):
        if not control:
            return False
        
        try:
            control.set_focus()
            control.type_keys(text)
            logging.info(f"输入文本成功: {text}")
            return True
        except Exception as e:
            logging.error(f"输入文本失败: {str(e)}")
            return False
    
    def set_text(self, control, text):
        if not control:
            return False
        
        try:
            control.set_text(text)
            logging.info(f"设置文本成功: {text}")
            return True
        except Exception as e:
            logging.error(f"设置文本失败: {str(e)}")
            return False
    
    def get_text(self, control):
        if not control:
            return None
        
        try:
            text = control.window_text()
            logging.info(f"获取文本成功: {text}")
            return text
        except Exception as e:
            logging.error(f"获取文本失败: {str(e)}")
            return None

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    ui_auto = UIAuto()
    if ui_auto.connect(title_re='网上股票交易系统5.0'):
        ui_auto.save_controls_to_file('controls.json')
#        control = ui_auto.get_control(ControlSelector(
#            title='网上股票交易系统5.0',
#            auto_id=None,
#            control_id=None,
#            class_name=None,
#            control_type=None,
#            found_index=None,
#        ))
#        control1 = ui_auto.get_control_by_path(control, path=[
#            ControlSelector(
#                title=None,
#                auto_id=None,
#                control_id=59392,
#                class_name='ToolbarWindow32',
#                control_type=None,
#                found_index=None,
#            ),
#            ControlSelector(
#                title=None,
#                auto_id=None,
#                control_id=1003,
#                class_name='ComboBox',
#                control_type=None,
#                found_index=None,
#            ),
#        ])
#        print(ui_auto.click(control1))
#        control2 = ui_auto.get_control_by_path(control, path=[
#            ControlSelector(
#                title=None,
#                auto_id=None,
#                control_id=59392,
#                class_name='ToolbarWindow32',
#                control_type=None,
#                found_index=None,
#            ),
#            ControlSelector(
#                title=None,
#                auto_id=None,
#                control_id=1003,
#                class_name='ComboBox',
#                control_type=None,
#                found_index=None,
#            ),
#        ])

        paths = ui_auto.find_control_path(ControlSelector(
            title=None,
            auto_id=None,
            control_id=None,
            class_name='Edit',
            control_type=None,
            found_index=0,
        ))

        print(f"找到控件路径: {dump_control_selectors(paths)}")
        # 通过路径获取控件
        control3 = ui_auto.get_control_by_path(None, paths)
        ui_auto.type_text(control3, '测试文本')

        # 使用示例：
        # ui_auto.click('[0][1][2]')  # 通过路径点击控件
        # ui_auto.type_text('[0][1][3]', '测试文本')  # 通过路径输入文本
        # text = ui_auto.get_text('[0][1][4]')  # 获取控件文本
