const Loader = ({ size = 'md', text = 'Cargando datos...' }) => {
  const sizes = {
    sm: 'w-5 h-5',
    md: 'w-8 h-8',
    lg: 'w-12 h-12'
  };

  return (
    <div className="flex flex-col items-center justify-center py-8 space-y-3">
      <div className={`animate-spin rounded-full border-b-2 border-blue-600 ${sizes[size]}`}></div>
      {text && <p className="text-sm text-slate-500 animate-pulse">{text}</p>}
    </div>
  );
};

export default Loader;