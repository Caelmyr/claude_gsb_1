"""
图谱存储模块 - JSON文件分片存储
"""
import json
import os
import threading
from typing import Dict, List, Optional
from backend.utils.config import GRAPH_DIR, GRAPH_SHARDS


class GraphStorage:
    """图谱存储管理器 - 按实体类型分片"""

    def __init__(self):
        # 使用可重入锁: add_relation 内部会调用 add_entity,
        # 普通 Lock 会导致线程等待自身持有的锁而永久死锁
        self.lock = threading.RLock()
        self._ensure_directories()
        self._cache = {}
        self._load_all_shards()

    def _ensure_directories(self):
        """确保目录存在"""
        os.makedirs(GRAPH_DIR, exist_ok=True)

    def _load_all_shards(self):
        """加载所有分片到缓存"""
        for entity_type, filename in GRAPH_SHARDS.items():
            filepath = os.path.join(GRAPH_DIR, filename)
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        self._cache[entity_type] = json.load(f)
                except (json.JSONDecodeError, ValueError, OSError):
                    # 分片文件损坏(如进程在写入中途被终止):
                    # 备份损坏文件并以空分片继续, 避免服务无法启动
                    corrupted_path = filepath + '.corrupted'
                    try:
                        os.replace(filepath, corrupted_path)
                    except OSError:
                        pass
                    self._cache[entity_type] = {'entities': {}, 'relations': []}
            else:
                self._cache[entity_type] = {'entities': {}, 'relations': []}

    def _save_shard(self, entity_type: str):
        """保存指定分片到文件(原子写入, 防止写入中途崩溃导致文件损坏)"""
        filename = GRAPH_SHARDS.get(entity_type, 'other.json')
        filepath = os.path.join(GRAPH_DIR, filename)
        tmp_path = filepath + '.tmp'
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(self._cache[entity_type], f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, filepath)

    def add_entity(self, entity_text: str, entity_type: str, properties: Dict = None):
        """添加实体"""
        with self.lock:
            if entity_type not in self._cache:
                self._cache[entity_type] = {'entities': {}, 'relations': []}

            if entity_text not in self._cache[entity_type]['entities']:
                self._cache[entity_type]['entities'][entity_text] = {
                    'id': f"{entity_type}_{len(self._cache[entity_type]['entities'])}",
                    'text': entity_text,
                    'type': entity_type,
                    'properties': properties or {},
                    'count': 1
                }
            else:
                self._cache[entity_type]['entities'][entity_text]['count'] += 1

            self._save_shard(entity_type)

    def add_relation(self, subject: str, subject_type: str, predicate: str,
                     obj: str, object_type: str, properties: Dict = None):
        """添加关系"""
        with self.lock:
            # 确保实体存在
            self.add_entity(subject, subject_type)
            self.add_entity(obj, object_type)

            # 添加关系到主语所在分片
            if subject_type not in self._cache:
                self._cache[subject_type] = {'entities': {}, 'relations': []}

            relation = {
                'subject': subject,
                'subject_type': subject_type,
                'predicate': predicate,
                'object': obj,
                'object_type': object_type,
                'properties': properties or {}
            }

            # 检查是否已存在
            existing = self._cache[subject_type]['relations']
            if not any(r['subject'] == subject and r['predicate'] == predicate and r['object'] == obj for r in existing):
                existing.append(relation)
                self._save_shard(subject_type)

    def get_entity(self, entity_text: str) -> Optional[Dict]:
        """获取实体信息"""
        with self.lock:
            for entity_type, shard in self._cache.items():
                if entity_text in shard['entities']:
                    return shard['entities'][entity_text]
            return None

    def get_entity_relations(self, entity_text: str) -> List[Dict]:
        """获取实体的所有关系"""
        relations = []
        with self.lock:
            for entity_type, shard in self._cache.items():
                for relation in shard['relations']:
                    if relation['subject'] == entity_text or relation['object'] == entity_text:
                        relations.append(relation)
        return relations

    def get_all_entities(self) -> List[Dict]:
        """获取所有实体"""
        entities = []
        with self.lock:
            for entity_type, shard in self._cache.items():
                entities.extend(shard['entities'].values())
        return entities

    def get_all_relations(self) -> List[Dict]:
        """获取所有关系"""
        relations = []
        with self.lock:
            for entity_type, shard in self._cache.items():
                relations.extend(shard['relations'])
        return relations

    def get_graph_data(self) -> Dict:
        """获取图谱可视化数据"""
        nodes = []
        links = []
        node_ids = set()

        with self.lock:
            for entity_type, shard in self._cache.items():
                for entity_text, entity_data in shard['entities'].items():
                    if entity_data['id'] not in node_ids:
                        node_ids.add(entity_data['id'])
                        nodes.append({
                            'id': entity_data['id'],
                            'label': entity_text,
                            'type': entity_type,
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
            for entity_type, shard in self._cache.items():
                for entity_text, entity_data in shard['entities'].items():
                    if keyword in entity_text:
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
