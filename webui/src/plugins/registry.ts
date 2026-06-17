import type { RouteRecordRaw } from 'vue-router';
import { Factory } from 'lucide-vue-next';
import type { NavItem } from '../types';

export interface WebuiPagePlugin {
  id: string;
  label: string;
  route: string;
  icon: NavItem['icon'];
  group: string;
  requiredCapabilities: string[];
  apiNamespace: string;
  routeRecord: RouteRecordRaw;
}

export const webuiPagePlugins: WebuiPagePlugin[] = [
  {
    id: 'mfg',
    label: 'MFG',
    route: '/apps/mfg',
    icon: Factory,
    group: 'Apps',
    requiredCapabilities: ['mfg.manufacturing.application', 'cowd.matrix.runtime'],
    apiNamespace: '/api/apps/mfg',
    routeRecord: {
      path: '/apps/mfg',
      component: () => import('../pages/MfgPage.vue'),
      meta: {
        pluginId: 'mfg',
        label: 'MFG',
        requiredCapabilities: ['mfg.manufacturing.application', 'cowd.matrix.runtime'],
        apiNamespace: '/api/apps/mfg',
      },
    },
  },
];

export const pluginRoutes: RouteRecordRaw[] = webuiPagePlugins.map((plugin) => plugin.routeRecord);

export const pluginNavItems: NavItem[] = webuiPagePlugins.map((plugin) => ({
  id: plugin.id as NavItem['id'],
  label: plugin.label,
  route: plugin.route,
  icon: plugin.icon,
  group: plugin.group,
}));
