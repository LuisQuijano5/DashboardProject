import React from 'react';

const Badge = ({ children, variant = 'default' }) => {
  const styles = {
    default: "bg-slate-100 text-slate-800",
    success: "bg-green-100 text-green-700",
    warning: "bg-yellow-100 text-yellow-700",
    danger: "bg-red-100 text-red-700",
    blue: "bg-blue-100 text-blue-700"
  };

  return (
    <span className={`px-2.5 py-0.5 rounded-full text-xs font-bold uppercase tracking-wide ${styles[variant] || styles.default}`}>
      {children}
    </span>
  );
};

export default Badge;