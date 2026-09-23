"""
图谱存储模块 - JSON文件分片存储
"""
import json
import os
import tempfile
import threading
from typing import Dict, List, Optional
from backend.utils.config import GRAPH_DIR, GRAPH_SHARDS


class GraphStorage:
    """图谱存储管理器 - 按实体类型分片"""

    def __init__(self):
        # 使用可重入锁：add_relation 内部需要调用 add_entity，
        # 同一线程可能重复进入加锁区，RLock 允许同线程多次获取。
        self.lock = threading.RLock()
        self._ensure_directories()
        self._cache = {}
        self._load_all_shards()

    def _ensure_directories(self):
        """确保目录存在"""
        os.makedirs(GRAPH_DIR, exist_ok=True)

    def _shard_key(self, entity_type: str) -> str:
        """将实体类型归一化到已配置的分片键，未知类型归入 OTHER"""
        if entity_type in GRAPH_SHARDS:
            return entity_type
        return 'OTHER'

    def _shard_filename(self, entity_type: str) -> str:
        key = self._shard_key(entity_type)
        return GRAPH_SHARDS.get(key, 'other.json')

    def _empty_shard(self) -> Dict:
        return {'entities': {}, 'relations': []}

    def _normalize_shard(self, data) -> Dict:
        """校验/修复分片数据结构"""
        if not isinstance(data, dict):
            return self._empty_shard()
        entities = data.get('entities')
        relations = data.get('relations')
        if not isinstance(entities, dict) or not isinstance(relations, list):
            return self._empty_shard()
        return {'entities': entities, 'relations': relations}

    def _load_all_shards(self):
        """加载所有分片到缓存。

        分片文件可能因历史版本的非原子写入或进程被强杀而损坏/截断，
        这里不能让单个坏文件导致整个系统无法启动（否则统计归零、
        已解析内容全部不可用）：坏文件先备份再按空分片加载。
        """
        for entity_type, filename in GRAPH_SHARDS.items():
            filepath = os.path.join(GRAPH_DIR, filename)
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                    if not content:
                        self._cache[entity_type] = self._empty_shard()
                    else:
                        self._cache[entity_type] = self._normalize_shard(json.loads(content))
                except (json.JSONDecodeError, OSError, ValueError) as e:
                    backup = f'{filepath}.corrupt'
                    try:
                        os.replace(filepath, backup)
                    except OSError:
                        pass
                    print(f'[GraphStorage] 分片文件损坏已备份: {filepath} -> {backup} ({e})')
                    self._cache[entity_type] = self._empty_shard()
            else:
                self._cache[entity_type] = self._empty_shard()

    def _save_shard(self, entity_type: str):
        """原子保存指定分片到文件（临时文件 + os.replace），避免写一半被中断导致文件损坏"""
        key = self._shard_key(entity_type)
        if key not in self._cache:
            self._cache[key] = self._empty_shard()
        filename = GRAPH_SHARDS.get(key, 'other.json')
        filepath = os.path.join(GRAPH_DIR, filename)
        fd, tmp_path = tempfile.mkstemp(
            prefix=f'.{filename}.', suffix='.tmp', dir=GRAPH_DIR
        )
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(self._cache[key], f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, filepath)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def _ensure_shard_locked(self, key: str):
        """调用方必须已持有 self.lock"""
        if key not in self._cache:
            self._cache[key] = self._empty_shard()

    def _add_entity_locked(self, entity_text: str, entity_type: str,
                           properties: Dict = None, save: bool = True):
        """添加实体的内部实现（不加锁，供 add_relation 在持锁状态下复用）"""
        key = self._shard_key(entity_type)
        self._ensure_shard_locked(key)

        if entity_text not in self._cache[key]['entities']:
            self._cache[key]['entities'][entity_text] = {
                'id': f"{key}_{len(self._cache[key]['entities'])}",
                'text': entity_text,
                'type': key,
                'properties': properties or {},
                'count': 1
            }
        else:
            self._cache[key]['entities'][entity_text]['count'] += 1

        if save:
            self._save_shard(key)

    def add_entity(self, entity_text: str, entity_type: str, properties: Dict = None):
        """添加实体"""
        with self.lock:
            self._add_entity_locked(entity_text, entity_type, properties, save=True)

    def add_relation(self, subject: str, subject_type: str, predicate: str,
                     obj: str, object_type: str, properties: Dict = None):
        """添加关系"""
        with self.lock:  # RLock：内部调用不会自死锁
            subject_key = self._shard_key(subject_type)
            object_key = self._shard_key(object_type)

            # 确保关系两端的实体存在（复用内部实现，不重复加锁/存盘）
            self._add_entity_locked(subject, subject_type, save=False)
            self._add_entity_locked(obj, object_type, save=False)

            self._ensure_shard_locked(subject_key)

            relation = {
                'subject': subject,
                'subject_type': subject_key,
                'predicate': predicate,
                'object': obj,
                'object_type': object_key,
                'properties': properties or {}
            }

            existing = self._cache[subject_key]['relations']
            is_new = not any(
                r['subject'] == subject
                and r['predicate'] == predicate
                and r['object'] == obj
                for r in existing
            )
            if is_new:
                existing.append(relation)

            # 一次性持久化涉及的分片：关系落在主语分片，
            # 宾语实体可能落在另一个类型分片，两个分片都保存。
            self._save_shard(subject_key)
            if object_key != subject_key:
                self._save_shard(object_key)

    def get_entity(self, entity_text: str) -> Optional[Dict]:
        """获取实体信息"""
        with self.lock:
            for shard in self._cache.values():
                if entity_text in shard['entities']:
                    return shard['entities'][entity_text]
        return None

    def get_entity_relations(self, entity_text: str) -> List[Dict]:
        """获取实体的所有关系"""
        relations = []
        with self.lock:
            for shard in self._cache.values():
                for relation in shard['relations']:
                    if relation['subject'] == entity_text or relation['object'] == entity_text:
                        relations.append(relation)
        return relations

    def get_all_entities(self) -> List[Dict]:
        """获取所有实体"""
        entities = []
        with self.lock:
            for shard in self._cache.values():
                entities.extend(shard['entities'].values())
        return entities

    def get_all_relations(self) -> List[Dict]:
        """获取所有关系"""
        relations = []
        with self.lock:
            for shard in self._cache.values():
                relations.extend(shard['relations'])
        return relations

    def get_graph_data(self) -> Dict:
        """获取图谱可视化数据"""
        nodes = []
        links = []
        node_ids = set()

        with self.lock:
            for shard in self._cache.values():
                for entity_data in shard['entities'].values():
                    if entity_data['id'] not in node_ids:
                        node_ids.add(entity_data['id'])
                        nodes.append({
                            'id': entity_data['id'],
                            'label': entity_data['text'],
                            'type': entity_data['type'],
                            'count': entity_data.get('count', 1)
                        })

                for relation in shard['relations']:
                    source_entity = self.get_entity(relation['subject'])
                    target_entity = self.get_entity(relation['object'])
                    if source_entity and target_entity:
                        links.append({
                            'source': source_entity['id'],
                            'target': target_entity['id'],
                            'label': relation['predicate']
                        })

        return {'nodes': nodes, 'links': links}

    def search_entities(self, keyword: str) -> List[Dict]:
        """搜索实体"""
        results = []
        with self.lock:
            for shard in self._cache.values():
                for entity_data in shard['entities'].values():
                    if keyword in entity_data['text']:
                        results.append(entity_data)
        return results

    def get_statistics(self) -> Dict:
        """获取图谱统计信息"""
        total_entities = 0
        total_relations = 0
        entity_counts = {}

        with self.lock:
            for entity_type, shard in self._cache.items():
                count = len(shard['entities'])
                entity_counts[entity_type] = count
                total_entities += count
                total_relations += len(shard['relations'])

        return {
            'total_entities': total_entities,
            'total_relations': total_relations,
            'entity_counts': entity_counts
        }
