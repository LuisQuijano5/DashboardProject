import React from 'react';
import { Card, CardContent } from '../ui/Card';

const KpiCard = ({ title, value, subtitle, color = "blue" }) => {
  const colorMap = {
    blue: "text-blue-600",
    green: "text-emerald-600",
    purple: "text-purple-600",
    red: "text-rose-600"
  };

  return (
    <Card className="hover:shadow-md transition-shadow duration-200">
      <CardContent className="pt-6">
        <p className="text-sm font-medium text-slate-500 uppercase tracking-wide">{title}</p>
        <div className="mt-2 flex items-baseline gap-2">
          <span className={`text-3xl font-bold ${colorMap[color]}`}>{value}</span>
        </div>
        <p className="text-xs text-slate-400 mt-1">{subtitle}</p>
      </CardContent>
    </Card>
  );
};

export default KpiCard;